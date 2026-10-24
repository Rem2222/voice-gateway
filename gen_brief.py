#!/usr/bin/env python3
"""Выжимка сессий Гермеса Романа за N дней для системного промта голосового шлюза.
Берёт из state.db: заголовки сессий + user-вопросы (+ первые символы assistant-ответов).
Безопасность: режет по длине, НЕ выводит содержимое tool-результатов и секреты
(ищет характерные маркеры и выбрасывает такие строки)."""
import sqlite3, sys, re, datetime

DB = '/root/.hermes/state.db'
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 1
MAX_MSG = 300        # символов на одно сообщение
MAX_ASST = 120
MAX_PER_SESSION = 8  # сообщений на сессию
MAX_SESSIONS = 40

SENSITIVE = re.compile(r'(alibaba|tokenplan|alibaba_tokenplan_key|api[-_]?key|password|secret|bearer|smart_hub|multica)',
                       re.I)

def shorten(s, n):
    s = re.sub(r'\s+', ' ', s or '').strip()
    return s if len(s) <= n else s[:n] + '…'

def main():
    since = datetime.datetime.now() - datetime.timedelta(days=DAYS)
    ts0 = since.timestamp()
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = cur.execute("""
        SELECT m.session_id, m.role, m.content, m.timestamp,
               s.title, s.source, s.started_at
        FROM messages m LEFT JOIN sessions s ON s.id = m.session_id
        WHERE m.timestamp >= ? AND m.role IN ('user','assistant')
          AND m.content IS NOT NULL AND length(m.content) > 3
        ORDER BY m.id
    """, (ts0,)).fetchall()

    sessions = {}
    for r in rows:
        sid = r['session_id']
        s = sessions.setdefault(sid, {'title': r['title'] or '', 'source': r['source'] or '',
                                      'items': []})
        if len(s['items']) >= MAX_PER_SESSION * 2:
            continue
        content = str(r['content'])
        if SENSITIVE.search(content):
            content = shorten(content, 60)  # обрезаем подозрительные
        s['items'].append((r['role'], content))

    out = []
    now = datetime.datetime.now().strftime('%d.%m %H:%M')
    out.append(f"Выжимка разговоров Романа с ассистентом за последние {DAYS} дн. (сгенерировано {now}).")
    out.append("Отвечай от имени ассистента, помни контекст:")
    used = 0
    for sid, s in sorted(sessions.items(), key=lambda kv: -len(kv[1]['items'])):
        if used >= MAX_SESSIONS:
            break
        title = s['title'] or sid[:8]
        header = f"• [{title}] (канал: {s['source'] or '?'})"
        out.append(header)
        n_user = 0
        for role, content in s['items']:
            if role == 'user' and n_user < MAX_PER_SESSION:
                out.append(f"  р: {shorten(content, MAX_MSG)}")
                n_user += 1
            elif role == 'assistant':
                out.append(f"  а: {shorten(content, MAX_ASST)}")
        used += 1

    text = "\n".join(out)
    print(text)

if __name__ == '__main__':
    main()