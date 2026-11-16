#!/usr/bin/env python3
"""Freefield's backend as a RunPod Serverless worker, behind a load-balancing endpoint.

The command of the image Dockerfile.runpod builds, which holds what infra/modal_app.py's image holds: ComfyUI at
the pinned commit with the pinned custom nodes, and comfy-api-proxy. This starts the same three processes, gateway
first: RunPod polls GET /ping on PORT and routes nothing to a worker until it answers 200, so the gateway answers 204
while ComfyUI boots, then 200 once ComfyUI :8188 and comfy-api-proxy :8189 are up. When any of the three exits, the
others are stopped and the worker exits with an error, so RunPod replaces it.

Requests reach a worker only through https://<ENDPOINT_ID>.api.runpod.ai, which wants your RunPod API key as
`Authorization: Bearer`: the header the gateway reads its own token from. RunPod's key is the lock, so the gateway
runs open here (FREEFIELD_ALLOW_OPEN=1), and on your machine FREEFIELD_TOKEN in .env is that RunPod API key.

Models come from the endpoint's network volume, mounted at /runpod-volume: they go in <volume>/models/<dir>/, which
a Pod with the same volume (mounted at /workspace there) fills with
    python tools/local_backend.py download /workspace/comfy <packs> --models /workspace/models

ComfyUI looks for models in a cache on the worker's local disk first (FREEFIELD_MODEL_CACHE, /models-local), then on
the volume. The gateway fills the cache when a run's /freefield/prepare names the app's files: a global volume is
object storage, which ComfyUI's mmap reads at ~28 MB/s, so each file is copied once, in one sequential read, before
the run (gateway.py, Warmer). The endpoint's container disk holds the cache: give it room for the apps you run.

Env: PORT and PORT_HEALTH (RunPod's, both 80 by default; they must be the same port here), FREEFIELD_COMFY_ARGS
(extra ComfyUI flags), FREEFIELD_PUBLIC_URL (the address links in job answers are written with; defaults to
https://$RUNPOD_ENDPOINT_ID.api.runpod.ai when RunPod sets RUNPOD_ENDPOINT_ID, else the Host each request came with),
FREEFIELD_MODEL_CACHE (an empty value turns the cache off).
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from tools.local_backend import MODEL_DIRS, find_proxy, wait_http, wait_port  # noqa: E402

COMFY_DIR = Path(os.environ.get("FREEFIELD_COMFY_DIR", "/comfyui"))
MODELS_DIR = Path(os.environ.get("FREEFIELD_MODELS_DIR", "/runpod-volume/models"))
CACHE_DIR = os.environ.get("FREEFIELD_MODEL_CACHE", "/models-local")


def model_paths_yaml(path: Path) -> Path:
    """extra_model_paths for ComfyUI: the local cache first (is_default puts it ahead of every other path),
    then the volume, which has every file."""
    sections = []
    if CACHE_DIR:
        for d in MODEL_DIRS:
            os.makedirs(os.path.join(CACHE_DIR, d), exist_ok=True)
        sections.append(["cache:", f"  base_path: {CACHE_DIR}", "  is_default: true"])
    sections.append(["volume:", f"  base_path: {MODELS_DIR}"])
    path.write_text("".join("\n".join(s + [f"  {d}: {d}" for d in MODEL_DIRS]) + "\n" for s in sections),
                    encoding="utf-8")
    return path


def public_url() -> str:
    if os.environ.get("FREEFIELD_PUBLIC_URL"):
        return os.environ["FREEFIELD_PUBLIC_URL"].strip().rstrip("/")
    endpoint = os.environ.get("RUNPOD_ENDPOINT_ID", "").strip()
    return f"https://{endpoint}.api.runpod.ai" if endpoint else ""


def main() -> None:
    port = os.environ.get("PORT", "80")
    if os.environ.get("PORT_HEALTH", port) != port:
        sys.exit(f"PORT_HEALTH ({os.environ['PORT_HEALTH']}) differs from PORT ({port}); the gateway answers both "
                 "the app and the health check on one port, so set them to the same value (or leave both unset: 80)")
    proxy = find_proxy()
    if not proxy:
        sys.exit("comfy-api-proxy is not installed in this image")
    public = public_url()
    if MODELS_DIR.is_dir():
        print(f"models from {MODELS_DIR}", flush=True)
    else:
        print(f"WARNING: {MODELS_DIR} does not exist: attach the network volume that holds the models to this endpoint; "
              "only apps without models (smoke) will run", flush=True)

    def stop(*_):   # RunPod stops a worker with SIGTERM: stop all three, as Ctrl+C does
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    procs: list[subprocess.Popen] = []
    try:
        paths = model_paths_yaml(Path("/tmp/freefield-model-paths.yaml"))
        env = {**os.environ, "FREEFIELD_ALLOW_OPEN": "1", "FREEFIELD_PUBLIC_URL": public,
               "COMFYUI_URL": "http://127.0.0.1:8188", "PROXY_URL": "http://127.0.0.1:8189", "GATEWAY_PORT": port,
               "FREEFIELD_MODEL_SOURCE": str(MODELS_DIR) if CACHE_DIR else "", "FREEFIELD_MODEL_CACHE": CACHE_DIR}
        env.pop("FREEFIELD_TOKEN", None)   # RunPod's API key is the lock (see above)
        procs.append(subprocess.Popen([sys.executable, str(ROOT / "infra" / "gateway.py")], env=env))
        wait_port(int(port), procs[-1])
        print(f"gateway on :{port}, answering /ping with 204 until ComfyUI is up; links written as "
              f"{public or 'the Host of each request'}; model cache {CACHE_DIR or 'off'}", flush=True)
        cmd = [sys.executable, "main.py", "--listen", "127.0.0.1", "--port", "8188", "--disable-auto-launch",
               "--extra-model-paths-config", str(paths),
               *os.environ.get("FREEFIELD_COMFY_ARGS", "").split()]
        procs.append(subprocess.Popen(cmd, cwd=COMFY_DIR))
        wait_http("http://127.0.0.1:8188/system_stats", procs[-1])
        procs.append(subprocess.Popen([proxy, "--comfyui", "http://127.0.0.1:8188", "--port", "8189",
                                       "--comfyui-base-dir", str(COMFY_DIR)]))
        wait_port(8189, procs[-1])
        print("worker ready: /ping answers 200", flush=True)
        while all(p.poll() is None for p in procs):
            time.sleep(1)
        sys.exit("a backend process exited; the worker stops so RunPod replaces it")
    except KeyboardInterrupt:
        pass
    finally:
        for p in reversed(procs):
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
