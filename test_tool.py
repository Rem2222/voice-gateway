#!/usr/bin/env python3
"""Тест тула search_memory через шлюз: текстовый запрос -> функция -> ответ."""
import asyncio, json, sys
import websockets

GW = "ws://127.0.0.1:8799"

async def main():
    q = sys.argv[1] if len(sys.argv) > 1 else "Найди в моей памяти, что за проект smiths-chat и когда я его разворачивал"
    async with websockets.connect(GW, open_timeout=10) as ws:
        await ws.send(json.dumps({"type":"start"}))
        while True:
            msg = json.loads(await asyncio.wait_for(ws.recv(), 30))
            if msg.get("type") == "ready":
                print("READY"); break
        await ws.send(json.dumps({"type":"text", "text": q}))
        out = bytearray(); text = []
        while True:
            msg = json.loads(await asyncio.wait_for(ws.recv(), 180))
            t = msg.get("type")
            if t == "audio":
                out.extend(base64_decode(msg["b64"]))
            elif t == "text":
                text.append(msg.get("delta",""))
            elif t == "tool":
                print(f"TOOL ВЫЗВАН: {msg.get('name')} args={msg.get('args')}")
                print(f"  (полный печать args raw: {msg.get('args')})")
            elif t == "tool_started":
                print(f"TOOL ИСПОЛНЯЕТСЯ: {msg.get('name')} args={msg.get('args')}")
            elif t == "done":
                print(f"DONE status={msg.get('status')}")
                print("ОТВЕТ:", "".join(text))
                break
            elif t == "error":
                print("ERROR:", msg); break

import base64 as base64_module
def base64_decode(s): return base64_module.b64decode(s)

asyncio.run(main())