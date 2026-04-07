from __future__ import annotations

import os
import traceback
import uuid
import base64
import logging
from typing import Dict

from e2b_code_interpreter import Sandbox

logger = logging.getLogger(__name__)

# Create a local directory to persist charts for the Synthesizer
LOCAL_CHART_DIR = "/tmp/temp_charts"
os.makedirs(LOCAL_CHART_DIR, exist_ok=True)

# Packages to pre-install inside the E2B sandbox so user code doesn't fail
_SANDBOX_DEPS = ["pyarrow", "matplotlib", "seaborn"]


def e2b_executor_tool(python_code: str, data_file_path: str) -> Dict[str, str | None]:
    """
    Executes python_code inside an E2B sandbox.

    Workflow:
      1. Create sandbox & install required packages (pyarrow, matplotlib, etc.)
      2. Upload the data file
      3. Patch local paths → remote paths in the code
      4. Execute the code
      5. Capture stdout + download any generated .png charts
      6. Return results (always includes stdout, even on error)
    """
    sbx = None
    local_chart_paths = []

    try:
        sbx = Sandbox.create()
        logger.info("[E2B] Sandbox created")

        # ── 0. Install missing dependencies inside the sandbox ──────────
        # fastparquet is more stable than pyarrow for re-installation in E2B
        install_code = "!pip install -q fastparquet matplotlib seaborn 2>/dev/null"
        logger.info("[E2B] Installing sandbox deps: fastparquet, matplotlib, seaborn")
        sbx.run_code(install_code, timeout=30)

        # ── 1. Upload Data ──────────────────────────────────────────────
        patched_code = python_code
        if data_file_path and str(data_file_path).strip():
            # Verify it actually exists as a file to prevent FileNotFoundError
            if os.path.exists(data_file_path) and os.path.isfile(data_file_path):
                remote_path = f"/home/user/{os.path.basename(data_file_path)}"
                try:
                    with open(data_file_path, "rb") as f:
                        sbx.files.write(remote_path, f)
                    logger.info("[E2B] Uploaded %s → %s", data_file_path, remote_path)
                    # ── 2. Patch paths ──────────────────────────────────────────────
                    patched_code = python_code.replace(data_file_path, remote_path)
                except Exception as upload_err:
                    logger.error("[E2B] Failed to read/upload data file %s: %s", data_file_path, upload_err)
            else:
                logger.warning("[E2B] data_file_path '%s' does not exist or is not a file. Skipping file upload.", data_file_path)
        else:
            logger.warning("[E2B] data_file_path is missing or empty string. Skipping file upload.")

        # ── 3. Execute ──────────────────────────────────────────────────
        execution = sbx.run_code(patched_code, timeout=60)

        # ── 4. Capture Stdout ───────────────────────────────────────────
        # Robustly capture stdout lines from different possible E2B SDK object formats
        stdout_lines = []
        for out in execution.logs.stdout:
            if hasattr(out, "line"):
                stdout_lines.append(out.line)
            else:
                stdout_lines.append(str(out))
        stdout = "\n".join(stdout_lines).strip()

        # ── 5. Capture and Persist Charts ───────────────────────────────
        explicit_charts = []
        # A. Files saved by plt.savefig inside the sandbox
        try:
            files = sbx.files.list("/home/user")
            for file_info in files:
                if file_info.name.endswith(".png") and file_info.path != remote_path:
                    content = sbx.files.read(file_info.path, format="bytes")
                    local_path = os.path.join(LOCAL_CHART_DIR, os.path.name(file_info.path) if hasattr(os.path, 'name') else os.path.basename(file_info.path))
                    with open(local_path, "wb") as f:
                        f.write(content)
                    explicit_charts.append(local_path)
        except Exception as fs_err:
            logger.warning("[E2B] Could not scan sandbox filesystem: %s", fs_err)

        if explicit_charts:
            local_chart_paths.extend(explicit_charts)
        else:
            # B. Inline plot outputs (implicit plt.plot without savefig)
            for i, result in enumerate(execution.results):
                if hasattr(result, "png") and result.png:
                    img_data = result.png
                    if isinstance(img_data, str):
                        img_data = base64.b64decode(img_data)

                    local_path = os.path.join(LOCAL_CHART_DIR, f"chart_{uuid.uuid4().hex[:8]}.png")
                    with open(local_path, "wb") as f:
                        f.write(img_data)
                    local_chart_paths.append(local_path)

        # ── 6. Build result ─────────────────────────────────────────────
        res = {
            "execution_result": stdout or "No stdout recorded.",
            "error_traceback":  None,
            "chart_paths":      ",".join(local_chart_paths),
        }

        if execution.error:
            err_name = execution.error.name or ""
            # SystemExit is not a real error — it's how the code exits early
            # (e.g. empty dataframe guard).  Treat it as success if we got stdout.
            if err_name == "SystemExit" and stdout:
                logger.info("[E2B] Ignoring SystemExit (code exited cleanly with stdout)")
            else:
                res["error_traceback"] = (
                    f"{err_name}: {execution.error.value}\n"
                    f"{''.join(execution.error.traceback)}"
                )

        return res

    except Exception as exc:
        return {
            "execution_result": "",
            "error_traceback":  f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            "chart_paths":      "",
        }

    finally:
        if sbx:
            try:
                sbx.kill()
            except Exception:
                pass