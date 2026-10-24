#!/usr/bin/env python3
"""HTTP-сервер выжимки: GET / -> текст выжимки за N дней (кэш 30 мин)."""
import http.server, subprocess, os, time

CACHE = '/tmp/daily_brief.txt'
DAYS_DEFAULT = 1

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/' and not self.path.startswith('/?days='):
            self.send_error(404); return
        days = DAYS_DEFAULT
        if '=' in self.path:
            try: days = int(self.path.split('=')[1])
            except Exception: pass
        fresh = False
        try:
            fresh = time.time() - os.path.getmtime(CACHE) < 1800
        except FileNotFoundError:
            pass
        if not fresh:
            r = subprocess.run(['/usr/bin/python3', '/root/voice-gateway/gen_brief.py', str(days)],
                               capture_output=True, text=True, timeout=60)
            body = r.stdout if r.returncode == 0 else f"Ошибка: {r.stderr[:500]}"
            open(CACHE, 'w').write(body)
        else:
            body = open(CACHE).read()
        body = body.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

if __name__ == '__main__':
    http.server.ThreadingHTTPServer(('127.0.0.1', 8798), H).serve_forever()