# minicelery

Небольшая очередь на asyncio, написанная с нуля, чтобы разобраться,
как устроены Celery и подобные системы изнутри: брокер, воркеры, подтверждения,
повторы, планировщик.


## Статус

| Этап | Что | Статус |
|---|---|---|
| 1 | Брокер, задачи, воркер, результаты, CLI | 🟡 в работе |
| 2 | Надёжность: перехват задач, ретраи, DLQ | ⬜ не начат |
| 3 | Планировщик по cron | ⬜ не начат |
| 5 | Метрики и веб-панель | ⬜ не начат |

<!-- Статусы: ⬜ не начат · 🟡 в работе · ✅ готово -->

## Что почитать

**Redis Streams**
- [Введение в Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/): группы, pending, подтверждения
- Команды: [XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/) ·
  [XPENDING](https://redis.io/docs/latest/commands/xpending/) ·
  [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/) ·
  [XACK](https://redis.io/docs/latest/commands/xack/)

**Как это сделано в Celery**
- [task_acks_late](https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-acks-late): подтверждение после выполнения и его цена

**Ретраи**
- [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/), AWS Architecture Blog

**Книги**
- М. Клеппман, [Designing Data-Intensive Applications](https://dataintensive.net/): главы про очереди и семантику доставки
- Документация Redis: Streams, `XREADGROUP`, `XPENDING`, `XAUTOCLAIM`, https://habr.com/ru/articles/456270/?ysclid=muy977w0d810132422
- Документация Celery: `acks_late`, visibility timeout
- AWS Architecture Blog: «Exponential Backoff And Jitter»
- М. Клеппман, «Designing Data-Intensive Applications»: главы про очереди и семантику доставки

**Статьи**
- Habr, [Redis Stream — надёжность и масштабируемость ваших систем сообщений](https://habr.com/ru/articles/456270/?ysclid=muy977w0d810132422): главы про очереди и семантику доставки