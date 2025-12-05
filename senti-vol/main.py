import os
import sys
import json
import logging
import subprocess
import traceback
from datetime import datetime, timezone

from flask import Response


logger = logging.getLogger("senti-vol-ingest")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

# Map source script file
SCRIPT_MAP = {
    "fred": "fred_ingest.py",
    "market": "market_ingest.py",
    "news": "news_ingest.py",
    "reddit": "reddit_ingest.py",
    "youtube": "youtube_ingest.py",
    "finnhub": "finnhub_ingest.py",
    "yahoo": "yahoonews_ingest.py",  
}

def _now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def _error_response(status: int, message: str, extra: dict | None = None) -> Response:
    body = {"error": message, "status": status}
    if extra:
        body.update(extra)
    return Response(
        json.dumps(body, ensure_ascii=False),
        status=status,
        mimetype="application/json",
    )

def _run_script(script_name: str) -> dict:
   
    cmd = [sys.executable, script_name]
    logger.info("Starting subprocess: %s", " ".join(cmd))

    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except Exception as e:
        logger.exception("Subprocess failed to start: %s", e)
        return {
            "script": script_name,
            "exit_code": -1,
            "stdout": "",
            "stderr": f"Failed to start script: {e}",
            "traceback": traceback.format_exc(),
            "ts": _now_utc_iso(),
        }

    return {
        "script": script_name,
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "ts": _now_utc_iso(),
    }

def ingest(request):
    
   # Cloud Function HTTP entry point.
   #?source=<fred|market|news|reddit|youtube|finnhub|yahoo>

    source = None
    try:
        source = request.args.get("source")
    except Exception:
        source = None

    if not source:
        try:
            data = request.get_json(silent=True) or {}
            source = data.get("source")
        except Exception:
            source = None

    if not source:
        return _error_response(400, "Missing 'source' parameter")

    source = source.lower().strip()
    script = SCRIPT_MAP.get(source)
    if not script:
        return _error_response(
            400,
            f"Invalid source '{source}'. "
            f"Valid sources: {', '.join(sorted(SCRIPT_MAP.keys()))}",
        )

    # Run the ingestion script
    result = _run_script(script)

    # If script failed return 500 so Scheduler can alert
    status = 200 if result.get("exit_code") == 0 else 500

    # For debugging, include traceback only on failure if present
    if status != 200 and "traceback" not in result:
        result["traceback"] = None

    return Response(
        json.dumps(result, ensure_ascii=False),
        status=status,
        mimetype="application/json",
    )
