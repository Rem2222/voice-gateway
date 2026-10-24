#!/usr/bin/env python3
"""Тест шлюза: полный цикл start -> аудио -> commit -> ответ + тул search_memory.
Сохраняет ответный аудио PCM в /tmp/gw_out.pcm для прослушивания."""
import asyncio, base64, json, sys, re
import websockets

GW = "ws://127.0.0.1:8799"

async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "voice"
    raw = open('/tmp/roman_voice_16k.pcm','rb').read()
    chunk = raw[:10*32000]  # 10s голоса
    async with websockets.connect(GW, open_timeout=10) as ws:
        await ws.send(json.dumps({"type":"start", "voice":"longanxiaoxin"}))
        print("sent start")
        while True:
            msg = json.loads(await asyncio.wait_for(ws.recv(), 30))
            if msg.get("type") == "ready":
                print("READY")
                break
            print("pre:", msg)
        # шлём аудио
        for i in range(0, len(chunk), 8000):
            await ws.send(json.dumps({"type":"audio","b64":base64.b64encode(chunk[i:i+8000]).decode()}))
        await ws.send(json.dumps({"type":"commit"}))
        print("sent audio+commit, жду ответ...")
        out = bytearray()
        text = []
        while True:
            msg = json.loads(await asyncio.wait_for(ws.recv(), 120))
            t = msg.get("type")
            if t == "audio":
                out.extend(base64.b64decode(msg["b64"]))
                print(f"  audio delta +{len(base64.b64decode(msg['b64']))} b (всего {len(out)})")
            elif t == "text":
                text.append(msg.get("delta",""))
                print(f"  text delta: {msg.get('delta','')!r}")
            elif t == "tool_started":
                print(f"  TOOL: {msg.get('name')} args={msg.get('args')}")
            elif t == "done":
                print(f"DONE status={msg.get('status')}")
                print("ИТОГ ТЕКСТ:", "".join(text))
                break
            elif t == "error":
                print("ERROR:", msg)
                break
        if out:
            open('/tmp/gw_out.pcm','wb').write(bytes(out))
            print(f"аудио сохранено /tmp/gw_out.pcm {len(out)} b ({len(out)/24000:.1f}s @24k)")

asyncio.run(main())