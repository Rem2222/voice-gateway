#!/usr/bin/env python3
"""Voice Gateway: браузер(PWA) <-> realtime qwen-audio через WS.
- Клиент: wss://rem2222.top/rt/ws (за Authelia), JSON-сообщения.
- Сервер шлюза сам держит realtime-сессию Али, исполняет function calls
  (OpenViking REST, потом Multica), возвращает голос и текст клиенту.
Ключ ALIBABA_TOKENPLAN_KEY читается из /root/.hermes/.env, наружу не уходит.
"""
import asyncio, base64, json, os, re, ssl, sys, time, urllib.request, urllib.error, urllib.parse
import websockets
from websockets.datastructures import Headers
from websockets.http11 import Response

ENV_PATH = "/root/.hermes/.env"

def load_key():
    env = open(ENV_PATH).read()
    m = re.search(r'^ALIBABA_TOKENPLAN_KEY=(.*)$', env, re.M)
    if not m:
        print("FATAL: ALIBABA_TOKENPLAN_KEY not found", file=sys.stderr); sys.exit(1)
    return m.group(1).strip()

ALI_KEY = load_key()
ALI_WS = "wss://token-plan.ap-southeast-1.maas.aliyuncs.com/api-ws/v1/realtime?model=qwen-audio-3.0-realtime-plus"
OV_URL = "http://127.0.0.1:8900"
ALI_HOST = "token-plan.ap-southeast-1.maas.aliyuncs.com"

# ---------- Статистика для /health ----------
STARTED_AT = time.time()
STATS = {"clients": 0,            # открытых WS-клиентов (браузеров)
         "sessions": 0,           # живых realtime-сессий Али
         "ok": 0,                 # удачных подключений к апстриму
         "fail": 0,               # неудачных подключений к апстриму
         "last_ok": None,         # ts последнего удачного подключения
         "last_fail": None,       # ts последней неудачи подключения
         "last_error": None,      # текст последней ошибки апстрима
         "ali_errors": 0,         # error-событий от Aliyun во время сессий
         "tools": 0}              # исполненных tool-вызовов

def _age(ts):
    return None if not ts else round(time.time() - ts, 1)

async def probe_upstream(timeout=5):
    """TCP+TLS-проба апстрима Али: виден ли хост вообще (обрыв сети/DNS/TLS)."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ALI_HOST, 443, ssl=ssl.create_default_context(),
                                    server_hostname=ALI_HOST), timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, None
    except Exception as e:
        return False, str(e)[:200]


def ov_headers():
    try:
        k = open("/root/.openviking/api-key.txt").read().strip()
    except Exception:
        k = ""
    return {"Authorization": f"Bearer {k}",
            "X-OpenViking-Account": "default",
            "X-OpenViking-User": "default",
            "Content-Type": "application/json"}

DEFAULT_INSTRUCTIONS = (
    "Ты — голосовой ассистент Романа. Отвечай ВСЕГДА на русском языке, "
    "кратко и по делу, максимум 2-3 предложения. Если просят найти что-то "
    "в памяти или вспомнить — используй инструмент search_memory. "
    "Не выдумывай факты, о которых не уверен."
)
DEFAULT_VOICE = "longanxiaoxin"
DEFAULT_TOOLS = [
    {"type": "function", "name": "search_memory",
     "description": "Семантический поиск по долговременной памяти Романа (OpenViking). Вызывай когда просят найти/вспомнить информацию.",
     "parameters": {"type": "object",
                    "properties": {"query": {"type": "string", "description": "поисковый запрос"},
                                   "limit": {"type": "integer", "description": "сколько результатов (до 10)"}},
                    "required": ["query"]}},
    {"type": "function", "name": "viking_read",
     "description": "Прочитать полный текст памяти по URI viking://...",
     "parameters": {"type": "object",
                    "properties": {"uri": {"type": "string"}, "level": {"type": "string"}},
                    "required": ["uri"]}},
]

# ---------- Исполнители тулов ----------
def _http_json(method, url, headers=None, body=None, timeout=30):
    req = urllib.request.Request(url, method=method, headers=headers or {})
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(req, data=data, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return {"status": "error", "error": {"message": f"HTTP {e.code}: {e.read()[:200].decode(errors='replace')}"}}
    except Exception as e:
        return {"status": "error", "error": {"message": str(e)}}

async def ov_search(query, limit=10):
    data = await asyncio.to_thread(
        _http_json, "POST", f"{OV_URL}/api/v1/search/search", ov_headers(),
        {"query": query, "limit": min(int(limit or 10), 10)})
    if data.get("status") == "error":
        return f"Error: {data.get('error',{}).get('message','unknown')}"
    mems = data.get("result", {}).get("memories", [])
    if not mems:
        return "Ничего не найдено."
    lines = []
    for m in mems[:10]:
        lines.append(f"[{m.get('score',0):.3f}] {m.get('uri','')}\n{(m.get('abstract') or m.get('content') or '')[:250]}")
    return "\n\n".join(lines)

async def ov_read(uri, level="overview"):
    url = f"{OV_URL}/api/v1/content/read?uri={urllib.parse.quote(uri)}&level={urllib.parse.quote(level)}"
    data = await asyncio.to_thread(
        _http_json, "GET", url, ov_headers(), None, 30)
    if data.get("status") == "error":
        return f"Error: {data.get('error',{}).get('message','unknown')}"
    res = data.get("result", "")
    return (json.dumps(res, ensure_ascii=False) if isinstance(res, dict) else str(res))[:8000]

TOOL_IMPL = {"search_memory": ov_search, "viking_read": ov_read}

# ---------- Realtime-сессия ----------
class RealtimeSession:
    def __init__(self, ws_client, instructions, voice, tools):
        self.ws_client = ws_client  # отправка сообщений клиенту
        self.instructions = instructions
        self.voice = voice
        self.tools = tools
        self.ws = None
        self.sending = False  # идёт ответ модели (блокируем ввод пользователя)
        self._run_lock = False

    async def connect(self):
        self.ws = await websockets.connect(ALI_WS,
            additional_headers={"Authorization": f"Bearer {ALI_KEY}"}, open_timeout=20)
        await self._recv()  # session.created
        su = {"type": "session.update", "session": {
            "modalities": ["text", "audio"],
            "input_audio_format": "pcm",
            "instructions": self.instructions,
            "voice": self.voice,
            "turn_detection": None,
            "tools": self.tools}}
        await self.ws.send(json.dumps(su))
        while True:
            ev = await self._recv()
            if ev and ev.get("type") == "session.updated":
                break

    async def _recv(self, timeout=120):
        try:
            return json.loads(await asyncio.wait_for(self.ws.recv(), timeout=timeout))
        except asyncio.TimeoutError:
            return None

    async def send_audio(self, b64):
        await self.ws.send(json.dumps({"type": "input_audio_buffer.append", "audio": b64}))

    async def commit_and_run(self):
        if self._run_lock:
            return
        self._run_lock = True
        self.sending = True
        try:
            await self.ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
            await self.ws.send(json.dumps({"type": "response.create"}))
            await self.run_response()
        finally:
            self._run_lock = False

    async def run_response(self):
        """Ждёт ответ модели, исполняет function calls, возвращает финальный done."""
        try:
            await self._run_response_loop()
        finally:
            self.sending = False

    async def _guarded_response(self):
        try:
            await self.run_response()
        finally:
            self._run_lock = False

    async def _run_response_loop(self):
        audio_n = 0
        audio_bytes = 0
        while True:
            ev = await self._recv()
            if ev is None:
                await self._send({"type": "error", "message": "Таймаут ответа Али"})
                break
            t = ev.get("type")
            if t == "response.audio.delta":
                d = ev.get("delta", "")
                audio_n += 1
                audio_bytes += len(d)
                await self._send({"type": "audio", "b64": d})
            elif t == "response.audio_transcript.delta":
                await self._send({"type": "text", "delta": ev.get("delta", "")})
            elif t == "response.function_call_arguments.done":
                await self._send({"type": "tool", "name": ev.get("name"), "args": ev.get("arguments")})
            elif t == "response.done":
                r = ev.get("response", {})
                print(f"[gw] ответ завершён: {r.get('status')}, аудио {audio_n} чанков / {audio_bytes/1024:.0f} KB", flush=True)
                fn = next((o for o in r.get("output", []) if o.get("type") == "function_call"), None)
                if fn:
                    await self._exec_tool(fn)
                    continue  # продолжить цикл: ждём продолжение ответа
                await self._send({"type": "done", "status": r.get("status")})
                break
            elif t == "error":
                print(f"[gw] ERROR от Али: {json.dumps(ev.get('error', {}), ensure_ascii=False)}", flush=True)
                STATS["ali_errors"] += 1
                STATS["last_error"] = json.dumps(ev.get("error", {}), ensure_ascii=False)[:300]
                await self._send({"type": "error", "message": json.dumps(ev.get("error", {}), ensure_ascii=False)})
                break

    async def _exec_tool(self, fn):
        name = fn.get("name")
        args = json.loads(fn.get("arguments") or "{}")
        call_id = fn.get("call_id") or fn.get("id") or f"fc_{name}"
        STATS["tools"] += 1
        print(f"[gw] tool call: {name} {json.dumps(args, ensure_ascii=False)}", flush=True)
        await self._send({"type": "tool_started", "name": name, "args": args})
        impl = TOOL_IMPL.get(name)
        if impl is None:
            out = f"Инструмент {name} не реализован в шлюзе."
        else:
            try:
                out = await impl(**args)
            except Exception as e:
                out = f"Ошибка вызова {name}: {e}"
        print(f"[gw] tool result ({len(str(out))} chars)", flush=True)
        item = {"type": "conversation.item.create",
                "item": {"type": "function_call_output", "call_id": call_id, "output": str(out)[:6000]}}
        await self.ws.send(json.dumps(item))
        await self.ws.send(json.dumps({"type": "response.create"}))

    async def _send(self, msg):
        await self.ws_client.send(json.dumps(msg, ensure_ascii=False))

    async def close(self):
        try:
            await self.ws.close()
        except Exception:
            pass

# ---------- Клиентский WS (браузер) ----------
async def handle_client(ws_client):
    sess = None
    counted = False   # эта сессия уже посчитана в STATS["sessions"]
    STATS["clients"] += 1
    try:
        async for raw in ws_client:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("type")
            if t == "start":
                if sess:
                    await sess.close()
                    if counted:
                        STATS["sessions"] = max(0, STATS["sessions"] - 1)
                        counted = False
                sess = RealtimeSession(
                    ws_client,
                    msg.get("instructions") or DEFAULT_INSTRUCTIONS,
                    msg.get("voice") or DEFAULT_VOICE,
                    msg.get("tools") or DEFAULT_TOOLS)
                try:
                    await sess.connect()
                except Exception as e:
                    # обрыв/отказ апстрима Aliyun — фиксируем для /health
                    STATS["fail"] += 1
                    STATS["last_fail"] = time.time()
                    STATS["last_error"] = str(e)[:300]
                    print(f"[gw] апстрим недоступен: {e}", flush=True)
                    await sess.close()
                    sess = None
                    await ws_client.send(json.dumps(
                        {"type": "error",
                         "message": f"Апстрим Aliyun недоступен: {e}"}))
                    continue
                STATS["ok"] += 1
                STATS["last_ok"] = time.time()
                STATS["sessions"] += 1
                counted = True
                await ws_client.send(json.dumps({"type": "ready"}))
            elif t == "audio" and sess:
                await sess.send_audio(msg.get("b64", ""))
            elif t == "text" and sess:
                # текстовый ввод (отладка/вебка): добавляем как сообщение пользователя
                await sess.ws.send(json.dumps({"type": "conversation.item.create", "item": {
                    "type": "message", "role": "user",
                    "content": [{"type": "input_text", "text": msg.get("text", "")}]}}))
                if not sess._run_lock:
                    sess._run_lock = True
                    sess.sending = True
                    await sess.ws.send(json.dumps({"type": "response.create"}))
                    asyncio.create_task(sess._guarded_response())
            elif t == "commit" and sess:
                if not sess.sending:
                    asyncio.create_task(sess.commit_and_run())
            elif t == "stop" and sess:
                break
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if sess:
            await sess.close()
        if counted:
            STATS["sessions"] = max(0, STATS["sessions"] - 1)
        STATS["clients"] = max(0, STATS["clients"] - 1)

# ---------- Health-endpoint (/health на 8799) ----------
async def health_payload():
    """JSON-статус шлюза: жив ли процесс, виден ли апстрим Али, отвечает ли OpenViking."""
    reachable, probe_err = await probe_upstream(5)
    ov_ok, ov_err = False, None
    t0 = time.time()
    data = await asyncio.to_thread(_http_json, "GET", f"{OV_URL}/health", None, None, 5)
    ov_ms = round((time.time() - t0) * 1000, 1)
    if isinstance(data, dict) and data.get("status") == "error":
        ov_err = str(data.get("error", {}).get("message", data))[:200]
    elif isinstance(data, dict):
        ov_ok = data.get("healthy") is True or data.get("status") in ("ok", "up")
    else:
        ov_err = str(data)[:200]

    # состояние апстрима по последней реальной попытке подключения
    if STATS["last_ok"] or STATS["last_fail"]:
        up_state = "ok" if (STATS["last_ok"] or 0) >= (STATS["last_fail"] or 0) else "error"
    else:
        up_state = "never"

    problems = []
    if not reachable:
        problems.append(f"апстрим Али недоступен по сети: {probe_err}")
    if up_state == "error":
        problems.append(f"последнее подключение к апстриму неудачно: {STATS['last_error']}")
    if not ov_ok:
        problems.append(f"OpenViking не отвечает: {ov_err}")

    return {"status": "degraded" if problems else "ok",
            "service": "voice-gateway",
            "uptime_sec": int(time.time() - STARTED_AT),
            "clients": STATS["clients"],
            "sessions": STATS["sessions"],
            "upstream": {"host": ALI_HOST, "reachable": reachable,
                         "probe_error": probe_err,
                         "state": up_state, "connects_ok": STATS["ok"],
                         "connects_failed": STATS["fail"],
                         "last_ok_sec_ago": _age(STATS["last_ok"]),
                         "last_fail_sec_ago": _age(STATS["last_fail"]),
                         "last_error": STATS["last_error"],
                         "session_errors": STATS["ali_errors"]},
            "openviking": {"healthy": ov_ok, "latency_ms": ov_ms, "error": ov_err},
            "tool_calls": STATS["tools"],
            "problems": problems}

async def process_request(connection, request):
    """HTTP-запрос на 8799: /health отвечаем JSON, остальное — обычный WS-handshake."""
    path = request.path.split("?", 1)[0]
    if path not in ("/health", "/healthz"):
        return None
    payload = await health_payload()
    body = json.dumps(payload, ensure_ascii=False).encode()
    headers = Headers([("Content-Type", "application/json; charset=utf-8"),
                       ("Content-Length", str(len(body))),
                       ("Cache-Control", "no-store")])
    code, reason = (200, "OK") if payload["status"] == "ok" else (503, "Service Unavailable")
    return Response(code, reason, headers, body)

async def main():
    print("Voice Gateway on 127.0.0.1:8799 (health: /health)", flush=True)
    async with websockets.serve(handle_client, "127.0.0.1", 8799,
                                max_size=10 * 1024 * 1024,
                                process_request=process_request):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())