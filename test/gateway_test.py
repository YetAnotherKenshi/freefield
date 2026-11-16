"""The gateway against a fake proxy: JSON links come back on the gateway's own address, streams pass through,
/ping says when ComfyUI is up.

    pip install aiohttp && python test/gateway_test.py
"""
import asyncio, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infra"))
os.environ["FREEFIELD_TOKEN"] = "t0k"
os.environ["PROXY_URL"] = "http://127.0.0.1:18189"
os.environ["COMFYUI_URL"] = "http://127.0.0.1:18188"
from aiohttp import web, ClientSession
import gateway  # infra/gateway.py

async def fake_proxy_app():
    async def job(request):
        base = f"{request.scheme}://{request.host}"
        return web.json_response({"id": "j1", "status": "succeeded",
                                  "urls": {"self": f"{base}/api/v2/jobs/j1", "events": f"{base}/api/v2/jobs/j1/events"},
                                  "outputs": [{"id": "o1", "url": f"{base}/api/v2/jobs/j1/outputs/o1"}]})
    async def events(request):
        resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await resp.prepare(request)
        await resp.write(b"data: {\"url\": \"http://127.0.0.1:18189/x\"}\n\n")
        await resp.write_eof(); return resp
    app = web.Application(); app.router.add_get("/api/v2/jobs/j1", job); app.router.add_get("/api/v2/jobs/j1/events", events)
    return app

async def main():
    proxy = web.AppRunner(await fake_proxy_app()); await proxy.setup(); await web.TCPSite(proxy, "127.0.0.1", 18189).start()
    gw = web.AppRunner(gateway.make_app()); await gw.setup(); await web.TCPSite(gw, "127.0.0.1", 18000).start()
    async with ClientSession() as s:
        h = {"Authorization": "Bearer t0k"}
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1", headers=h) as r:
            body = await r.json(); assert r.status == 200, r.status
        assert body["urls"]["self"] == "http://127.0.0.1:18000/api/v2/jobs/j1", body["urls"]
        assert body["outputs"][0]["url"] == "http://127.0.0.1:18000/api/v2/jobs/j1/outputs/o1"
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1", headers={**h, "X-Forwarded-Proto": "https", "Host": "gw.example.test"}) as r:
            body = await r.json()
        assert body["urls"]["self"] == "https://gw.example.test/api/v2/jobs/j1", body["urls"]
        # a public host with no forwarded header (Modal) is https; a local one keeps http
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1", headers={**h, "Host": "x--serve.us-east.modal.run"}) as r:
            body = await r.json()
        assert body["urls"]["self"] == "https://x--serve.us-east.modal.run/api/v2/jobs/j1", body["urls"]
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1", headers={**h, "Host": "192.168.1.20:8000"}) as r:
            body = await r.json()
        assert body["urls"]["self"] == "http://192.168.1.20:8000/api/v2/jobs/j1", body["urls"]
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1/events", headers=h) as r:
            text = await r.text(); assert r.headers["Content-Type"].startswith("text/event-stream")
        assert "http://127.0.0.1:18189/x" in text, "streams are passed through untouched"
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1") as r:
            assert r.status == 401, r.status
        # behind a load balancer that hands the worker its own Host, FREEFIELD_PUBLIC_URL writes the links
        gateway.PUBLIC_URL = "https://ep1.api.runpod.ai"
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1", headers={**h, "Host": "10.0.3.7:80"}) as r:
            body = await r.json()
        assert body["urls"]["self"] == "https://ep1.api.runpod.ai/api/v2/jobs/j1", body["urls"]
        gateway.PUBLIC_URL = ""
        # open (RunPod's API key is the lock): any bearer, or none, goes through
        gateway.ALLOW_OPEN = True
        async with s.get("http://127.0.0.1:18000/api/v2/jobs/j1", headers={"Authorization": "Bearer rpa_other"}) as r:
            assert r.status == 200, r.status
        gateway.ALLOW_OPEN = False
        # /ping, open: 204 while ComfyUI does not answer, 200 once it and the proxy do
        async with s.get("http://127.0.0.1:18000/ping") as r:
            assert r.status == 204, r.status
        async def system_stats(_):
            return web.json_response({"devices": []})
        comfy = web.Application(); comfy.router.add_get("/system_stats", system_stats)
        comfy_runner = web.AppRunner(comfy); await comfy_runner.setup(); await web.TCPSite(comfy_runner, "127.0.0.1", 18188).start()
        async with s.get("http://127.0.0.1:18000/ping") as r:
            assert r.status == 200, r.status
        await proxy.cleanup()
        async with s.get("http://127.0.0.1:18000/ping") as r:
            assert r.status == 204, f"{r.status}: the proxy is down, so not ready"
        await comfy_runner.cleanup()
    print("gateway relink and ping ok")
    await gw.cleanup()


async def warm():
    """With a model source and cache, prepare copies the app's files first and answers 202 until they are there."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        src, cache = Path(tmp, "volume"), Path(tmp, "cache")
        (src / "vae").mkdir(parents=True)
        (src / "vae" / "a.safetensors").write_bytes(b"x" * 1_000_000)
        gateway.MODEL_SOURCE, gateway.MODEL_CACHE = str(src), str(cache)
        gw = web.AppRunner(gateway.make_app()); await gw.setup(); await web.TCPSite(gw, "127.0.0.1", 18001).start()
        body = {"app": "x", "models": "vae/a.safetensors",
                "files": ["vae/a.safetensors", "../../etc/passwd", "/etc/passwd", "loras/not-on-the-volume.safetensors"]}
        h = {"Authorization": "Bearer t0k"}
        async with ClientSession() as s:
            async with s.post("http://127.0.0.1:18001/freefield/prepare", json=body, headers=h) as r:
                first = await r.json()
                assert r.status == 202 and first["pending"] == 1, (r.status, first)
            for _ in range(50):
                async with s.post("http://127.0.0.1:18001/freefield/prepare", json=body, headers=h) as r:
                    if r.status == 200:
                        break
                await asyncio.sleep(0.1)
            assert r.status == 200, r.status
        assert (cache / "vae" / "a.safetensors").read_bytes() == b"x" * 1_000_000
        assert not list(cache.rglob("*.part")), "the .part file is renamed when the copy is complete"
        assert not (cache / "etc").exists() and not Path(tmp, "etc").exists(), "only dir/name paths are copied"
        gateway.MODEL_SOURCE = gateway.MODEL_CACHE = ""
        await gw.cleanup()
    print("gateway model cache ok")

def copy_parts():
    """copy_file in several ranges gives the same bytes, for sizes that do not divide evenly and for an empty file."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        src, dst = Path(tmp, "src.safetensors"), Path(tmp, "out", "dst.safetensors")
        for size in (1_000_013, 0, 7):
            data = os.urandom(size)
            src.write_bytes(data)
            gateway.copy_file(src, dst, parts=4, min_part=100_000)
            assert dst.read_bytes() == data, size
            assert not dst.with_name(dst.name + ".part").exists()
    print("gateway copy in parts ok")

asyncio.run(main())
asyncio.run(warm())
copy_parts()
