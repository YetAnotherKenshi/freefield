"""The pass: the only door into the backend container.

    client (SDK) ──▶ gateway :8000 ──▶ comfy-api-proxy :8189 ──▶ ComfyUI :8188

- Every request needs `Authorization: Bearer $FREEFIELD_TOKEN` (the Comfy SDK sends its
  apiKey that way). The header is stripped before forwarding; the proxy has no auth.
- POST /freefield/prepare {"app", "models", "files"} is the pantry manager: when the requested
  app needs a different set of models than the one that ran last, it asks ComfyUI to
  unload everything (POST /free) so nothing from the previous app stays in VRAM.
  With FREEFIELD_MODEL_SOURCE and FREEFIELD_MODEL_CACHE set (RunPod), it first copies the
  app's `files` from the source volume to the cache on the worker's local disk and answers
  202 until they are there; ComfyUI looks in the cache first. RunPod's global volume is
  object storage: ComfyUI's mmap reads it at ~28 MB/s (a 1.5 GB VAE took 52 s, the first
  LTX-2.5 run spent 5 min on the 15 GB text encoder alone, 16 Nov), while large sequential
  reads, several at once, are what object storage is fast at (copy_file).
- GET /freefield/status shows the current app and ComfyUI's device stats.
- GET /health is open, for Modal's startup probe.
- GET /ping is open too, for load balancers that route only to ready workers (RunPod
  Serverless): 204 while ComfyUI or comfy-api-proxy is still starting, 200 once both answer.
- The proxy writes absolute links (a job's `urls.self`, `.events`, `.cancel`, an output's
  `url`) from the host *it* sees, http://127.0.0.1:8189; the SDK follows those links
  verbatim, so from outside they went nowhere ("fetch failed", 23 oct). JSON answers are
  rewritten to the address the client reached the gateway on, or to FREEFIELD_PUBLIC_URL
  when that is set (behind a load balancer whose Host header is not the public one).

Run: python gateway.py  (env: FREEFIELD_TOKEN, COMFYUI_URL, PROXY_URL, GATEWAY_PORT, FREEFIELD_PUBLIC_URL,
FREEFIELD_MODEL_SOURCE, FREEFIELD_MODEL_CACHE)
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout, web

COMFYUI_URL = os.environ.get("COMFYUI_URL", "http://127.0.0.1:8188")
PROXY_URL = os.environ.get("PROXY_URL", "http://127.0.0.1:8189")
TOKEN = os.environ.get("FREEFIELD_TOKEN", "")
ALLOW_OPEN = os.environ.get("FREEFIELD_ALLOW_OPEN") == "1"
PUBLIC_URL = os.environ.get("FREEFIELD_PUBLIC_URL", "").strip().rstrip("/")
MODEL_SOURCE = os.environ.get("FREEFIELD_MODEL_SOURCE", "")
MODEL_CACHE = os.environ.get("FREEFIELD_MODEL_CACHE", "")
OPEN_PATHS = {"/health", "/ping"}
HOP_HEADERS = {"host", "authorization", "content-length", "transfer-encoding", "connection", "keep-alive"}

log = logging.getLogger("gateway")


class Pantry:
    """Remembers which models are on the GPU and clears it when an app needs others."""

    def __init__(self, session: ClientSession):
        self.session = session
        self.models: str | None = None
        self.app: str | None = None
        self.lock = asyncio.Lock()
        self.frees = 0

    async def prepare(self, app: str, models: str) -> dict:
        async with self.lock:
            freed = False
            previous = self.app
            if self.models is not None and models != self.models:
                async with self.session.post(f"{COMFYUI_URL}/free", json={"unload_models": True, "free_memory": True}) as r:
                    r.raise_for_status()
                freed = True
                self.frees += 1
            self.models, self.app = models, app
            return {"freed": freed, "previous": previous, "current": app}


def copy_range(fi: int, fo: int, start: int, end: int, block: int = 16 << 20) -> None:
    """Bytes start..end of one open file into another, as one sequential stream."""
    off = start
    while off < end:
        data = os.pread(fi, min(block, end - off), off)
        if not data:
            raise EOFError(f"the source ended at byte {off}, before {end}")
        view, done = memoryview(data), 0
        while done < len(data):
            done += os.pwrite(fo, view[done:], off + done)
        off += len(data)


def copy_file(src: Path, dst: Path, parts: int = 8, min_part: int = 1 << 30) -> None:
    """src to dst in up to `parts` contiguous ranges read at once, into a .part file renamed when complete, so
    ComfyUI (which skips the .part extension) never sees half a model. RunPod's global volume serves one read
    stream at ~120 MB/s and several together at a few times that: from a CPU Pod on 16 Nov, one stream 123 MB/s,
    16 ranges of one file 229 MB/s, four files at once 301 MB/s in all."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    part = dst.with_name(dst.name + ".part")
    size = src.stat().st_size
    step = max(1, -(-size // max(1, min(parts, -(-size // min_part)))))
    fi = os.open(src, os.O_RDONLY)
    try:
        fo = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.ftruncate(fo, size)
            starts = range(0, size, step)
            with ThreadPoolExecutor(max(1, len(starts))) as pool:
                list(pool.map(lambda s: copy_range(fi, fo, s, min(s + step, size)), starts))
        finally:
            os.close(fo)
    finally:
        os.close(fi)
    os.replace(part, dst)


class Warmer:
    """Copies model files from a slow source volume to a local cache, a few at a time, each once."""

    def __init__(self, source: str, cache: str, parallel: int = 4):
        self.source, self.cache = Path(source), Path(cache)
        self.tasks: dict[str, asyncio.Task] = {}
        self.failed: dict[str, str] = {}
        self.sem = asyncio.Semaphore(parallel)

    @staticmethod
    def safe(rel: object) -> bool:
        """`dir/name` and nothing else: the list comes from the client."""
        if not isinstance(rel, str):
            return False
        parts = Path(rel).parts
        return len(parts) == 2 and not Path(rel).is_absolute() and ".." not in parts

    def pending(self, files: list) -> list[str]:
        """Files the source has and the cache does not, minus those that failed to copy (ComfyUI
        reads those from the source)."""
        return [rel for rel in dict.fromkeys(files) if self.safe(rel) and rel not in self.failed
                and not (self.cache / rel).is_file() and (self.source / rel).is_file()]

    def start(self, files: list[str]) -> None:
        """Largest first: the run waits for the last file, and the largest takes longest."""
        for rel in sorted(files, key=lambda r: -(self.source / r).stat().st_size):
            if rel not in self.tasks or self.tasks[rel].done():
                self.tasks[rel] = asyncio.get_running_loop().create_task(self._copy(rel))

    async def _copy(self, rel: str) -> None:
        async with self.sem:
            src = self.source / rel
            t0 = time.time()
            try:
                await asyncio.to_thread(copy_file, src, self.cache / rel)
            except Exception as e:  # a full disk, a vanished file: ComfyUI falls back to the source
                self.failed[rel] = f"{type(e).__name__}: {e}"
                log.warning("cache %s failed: %s", rel, self.failed[rel])
                return
            gb, s = src.stat().st_size / 1e9, max(time.time() - t0, 1e-3)
            log.info("cached %s: %.2f GB in %.0f s (%.0f MB/s)", rel, gb, s, gb * 1000 / s)


def authorized(request: web.Request) -> bool:
    if ALLOW_OPEN:
        return True
    header = request.headers.get("Authorization", "")
    return bool(TOKEN) and hmac.compare_digest(header, f"Bearer {TOKEN}")


@web.middleware
async def auth_mw(request: web.Request, handler):
    if request.path in OPEN_PATHS or authorized(request):
        return await handler(request)
    return web.json_response({"error": "missing or wrong token"}, status=401)


async def health(_: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def ping(request: web.Request) -> web.Response:
    """200 once ComfyUI and comfy-api-proxy both answer, 204 while either is still starting.
    The gateway starts first on RunPod (infra/runpod_worker.py), so the health check sees a
    worker that is booting, not one that is gone."""
    session: ClientSession = request.app["session"]
    quick = ClientTimeout(total=3)
    try:
        async with session.get(f"{COMFYUI_URL}/system_stats", timeout=quick) as r:
            if r.status != 200:
                return web.Response(status=204)
        async with session.get(f"{PROXY_URL}/", timeout=quick):
            pass   # any answer, a 404 included: it is listening
    except Exception:
        return web.Response(status=204)
    return web.json_response({"ok": True})


async def prepare(request: web.Request) -> web.Response:
    body = await request.json()
    if not isinstance(body.get("models"), str) or not isinstance(body.get("app"), str):
        return web.json_response({"error": "expected {app, models}"}, status=400)
    warmer: Warmer | None = request.app["warmer"]
    if warmer is not None and isinstance(body.get("files"), list):
        pending = warmer.pending(body["files"])
        if pending:   # the client asks again until this is empty
            warmer.start(pending)
            gb = sum((warmer.source / rel).stat().st_size for rel in pending) / 1e9
            return web.json_response({"warming": True, "pending": len(pending), "pending_gb": round(gb, 1)},
                                     status=202)
    result = await request.app["pantry"].prepare(body["app"], body["models"])
    log.info("prepare %s → %s", body["app"], result)
    return web.json_response(result)


async def status(request: web.Request) -> web.Response:
    pantry: Pantry = request.app["pantry"]
    stats = None
    try:
        async with pantry.session.get(f"{COMFYUI_URL}/system_stats") as r:
            stats = await r.json()
    except Exception as e:  # ComfyUI may still be starting
        stats = {"error": str(e)}
    return web.json_response({"app": pantry.app, "models": pantry.models, "frees": pantry.frees, "comfyui": stats})


LOCAL_HOSTS = ("127.0.0.1", "localhost", "0.0.0.0", "[::1]")


def external_base(request: web.Request) -> str:
    """Where the client reached us. Modal terminates TLS and talks plain http to the container
    without an X-Forwarded-Proto header (first deploy, 23 oct: links came out as http://…modal.run,
    the SDK saw another origin and sent no token, 401), so a public host is https unless a proxy
    says otherwise; only a local or private address keeps the scheme it was reached on.
    FREEFIELD_PUBLIC_URL, when set, wins: a load balancer may hand the worker a Host of its own."""
    if PUBLIC_URL:
        return PUBLIC_URL
    forwarded = request.headers.get("X-Forwarded-Proto", "").split(",")[0].strip()
    host = request.host
    name = host.rsplit(":", 1)[0] if not host.startswith("[") else host.split("]")[0] + "]"
    private = name in LOCAL_HOSTS or name.startswith(("10.", "192.168.", "172.16.", "172.17.", "172.18.", "172.19.", "172.2", "172.30.", "172.31."))
    scheme = forwarded or (request.scheme if private else "https")
    return f"{scheme}://{host}"


def relink(body: bytes, request: web.Request) -> bytes:
    """The proxy's own address in a JSON answer becomes the gateway's external one."""
    return body.replace(PROXY_URL.encode(), external_base(request).encode())


async def forward(request: web.Request) -> web.StreamResponse:
    session: ClientSession = request.app["session"]
    headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_HEADERS}

    async def body():
        async for chunk in request.content.iter_chunked(1 << 16):
            yield chunk

    data = body() if request.can_read_body else None
    async with session.request(request.method, f"{PROXY_URL}{request.rel_url}", headers=headers, data=data,
                               allow_redirects=False) as upstream:
        passthrough = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_HEADERS | {"content-encoding"}}
        if upstream.content_type == "application/json":   # job and asset answers carry links: fix them up whole
            raw = relink(await upstream.read(), request)
            passthrough.pop("Content-Length", None)
            return web.Response(status=upstream.status, body=raw, headers=passthrough)
        resp = web.StreamResponse(status=upstream.status)
        for k, v in passthrough.items():
            resp.headers[k] = v
        await resp.prepare(request)
        async for chunk in upstream.content.iter_any():   # streams SSE events and big files as they come
            await resp.write(chunk)
        await resp.write_eof()
        return resp


async def on_startup(app: web.Application) -> None:
    app["session"] = ClientSession(timeout=ClientTimeout(total=None, sock_connect=30), auto_decompress=False)
    app["pantry"] = Pantry(app["session"])
    app["warmer"] = Warmer(MODEL_SOURCE, MODEL_CACHE) if MODEL_SOURCE and MODEL_CACHE else None


async def on_cleanup(app: web.Application) -> None:
    await app["session"].close()


def make_app() -> web.Application:
    if not TOKEN and not ALLOW_OPEN:
        raise SystemExit("FREEFIELD_TOKEN is not set; refusing to start an open gateway")
    app = web.Application(middlewares=[auth_mw], client_max_size=1024 ** 3)
    app.router.add_get("/health", health)
    app.router.add_get("/ping", ping)
    app.router.add_post("/freefield/prepare", prepare)
    app.router.add_get("/freefield/status", status)
    app.router.add_route("*", "/{tail:.*}", forward)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


async def wait_for(url: str, timeout_s: float = 600) -> None:
    """Block until url answers 200, so Modal only routes traffic once ComfyUI is up."""
    deadline = time.time() + timeout_s
    async with ClientSession() as s:
        while time.time() < deadline:
            try:
                async with s.get(url) as r:
                    if r.status == 200:
                        return
            except Exception:
                pass
            await asyncio.sleep(1)
    raise TimeoutError(f"{url} did not come up in {timeout_s:.0f}s")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    web.run_app(make_app(), host="0.0.0.0", port=int(os.environ.get("GATEWAY_PORT", "8000")))
