# Voice Gateway

Голосовой шлюз Романа (поставлен 24.10.2026): браузерная PWA `https://rem2222.top/rt/`
говорит с realtime-моделью `qwen-audio-3.0-realtime-plus` (Aliyun MaaS), инструменты
ходят в OpenViking.

## Сервисы

| Юнит | Порт | Что делает |
|---|---|---|
| `voice-gateway.service` | `127.0.0.1:8799` | WS-шлюз: клиент ↔ realtime-сессия Али, исполнение function calls |
| `voice-brief.service` | `127.0.0.1:8798` | HTTP: выжимка сессий Hermes из `~/.hermes/state.db` для системного промта |

```bash
systemctl restart voice-gateway voice-brief
journalctl -u voice-gateway -u voice-brief -f
```

## Файлы

- `gateway.py` — WS-сервер 8799 + health-endpoint `/health`;
- `brief_server.py` — HTTP-сервер 8798 (`GET /`, `/?days=N`, кэш 30 мин);
- `gen_brief.py` — сборка выжимки из `state.db` (режет строки с маркерами секретов);
- `test_client.py` — полный голосовой цикл start → аудио → commit → ответ;
- `test_tool.py` — текстовый запрос через тулы (`search_memory`, `viking_read`).

## Health-endpoint (8799)

```bash
curl -s http://127.0.0.1:8799/health    # JSON, 200 = ok, 503 = degraded
```

Отдаёт: uptime, число клиентов/сессий, состояние апстрима Али
(TCP+TLS-проба хоста + статистика последних подключений), живость OpenViking,
число исполненных tool-вызовов и список `problems`. Прочие HTTP-пути на 8799
по-прежнему дают `426 InvalidUpgrade` (нужен WS-Upgrade), корневой `/` не тронут.

## Конфигурация и секреты

- `ALIBABA_TOKENPLAN_KEY` читается кодом из `/root/.hermes/.env`, наружу не уходит;
- ключ OpenViking — `/root/.openviking/api-key.txt`;
- **секреты в репозиторий не кладутся** — ротация ведётся через `/root/.secrets/ROTATION.md`.

## Выкладка

Репозиторий: https://github.com/Rem2222/voice-gateway (публичный — секретов в коде
нет, ключи читаются из файлов; wiki тоже публичная, `smiths-chat` — приватный).
Автокоммит+push при каждом ночном `pg-backup.sh` (секция «VOICE-GATEWAY GIT PUSH»).
