# CloudFunction

Serverless execution engine для VK Cloud — аналог AWS Lambda.

Пользователь загружает код функции, CloudFunction запускает его в полностью изолированном окружении с лимитами ресурсов и возвращает результат.

## Как это работает

```
Пользователь
    │  POST /functions/{id}/invoke
    ▼
API сервер  ──►  Kafka [invocations]  ──►  Executor
    │                                          │
    │                                     Изолированный
    │                                     запуск функции
    │                                          │
    └──  Kafka [results]  ◄───────────────────┘
    │
    ▼
Результат (stdout + exit code)
```

**API** принимает HTTP-запросы, хранит метаданные функций в PostgreSQL, передаёт задачи через Kafka и ждёт результата.

**Executor** читает задачи из Kafka, запускает функции в изолированном окружении с лимитами памяти, CPU и таймаутом, возвращает результат.

**Sandbox** — собственный execution engine, полная и компактная изоляция без Docker и без сторонних container runtime.

## Возможности

- Deploy функций через веб-интерфейс или REST API
- Runtimes: Python, Go, Java
- Лимиты памяти, CPU и таймаут на каждую функцию
- Параллельное выполнение через worker pool
- Результат: stdout + exit code
- Автоматическая очистка окружения после выполнения

## Структура проекта

```
CloudLambda/
  api/                  # HTTP сервер
    main.go             # старт, инициализация
    server.go           # handlers
    kafka.go            # producer, results reader, waitMap
    internal/
      db/               # PostgreSQL: подключение, миграции, репозиторий
      vars/             # типы (EnvType)
  executor/             # сервис исполнения
    cmd/
      main.go           # Kafka consumer, worker pool
    internal/
      sandbox/          # execution engine: подготовка окружения, запуск
      ns/               # изоляция процесса
      mount/            # файловая система
      params/           # конфигурация
  init.sh               # подготовка рантаймов (python, go, java)
  docker-compose.yml    # Kafka + PostgreSQL для локальной разработки
  playbook.yml          # Ansible деплой
  inventory.ini         # хосты
```

## Быстрый старт

### Зависимости

- Linux (Ubuntu 20.04+)
- Go 1.21+
- Java 21+ (для Kafka)
- Ansible (для деплоя)

### Деплой через Ansible

1. Укажи путь к проекту в `inventory.ini`:

```ini
[cloudfunctions]
localhost ansible_connection=local

[cloudfunctions:vars]
project_dir=/home/user/CloudLambda
```

2. Установи Ansible коллекции:

```bash
ansible-galaxy collection install community.docker community.postgresql
```

3. Запусти плейбук:

```bash
ansible-playbook -i inventory.ini playbook.yml -K
```

Плейбук установит все зависимости, подготовит рантаймы, соберёт бинари и запустит сервисы через systemd.

4. Открой веб-интерфейс: [http://localhost:8080](http://localhost:8080)

### Переменные окружения

**API:**

| Переменная | По умолчанию | Описание |
|---|---|---|
| `ADDR` | `:8080` | Адрес HTTP сервера |
| `DATABASE_URL` | — | PostgreSQL DSN |
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker |
| `FUNCTIONS_ROOT` | `/var/cloudfunctions/functions` | Директория для хранения кода функций |

**Executor:**

| Переменная | По умолчанию | Описание |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker |
| `WORKER_POOL_SIZE` | `10` | Максимум параллельных вызовов |
| `PYTHON_ENV_SRC` | — | Путь к Python runtime |
| `GO_ENV_SRC` | — | Путь к Go runtime |
| `JAVA_ENV_SRC` | — | Путь к Java runtime |

## REST API

### Задеплоить функцию

```
POST /functions
Content-Type: multipart/form-data

name=my-function
runtime=python          # python | go | java
memory_limit=536870912  # байты, по умолчанию 512MB
cpu_quota=50000         # мкс на 100мс, по умолчанию 50%
timeout_sec=10          # секунды
file=@script.py
```

### Вызвать функцию

```
POST /functions/{id}/invoke
```

Ответ:

```json
{
  "job_id": "a3f9c2b1",
  "exit_code": 0,
  "timed_out": false,
  "stdout": "Hello, World!\n"
}
```

### Другие методы

```
GET    /functions          # список функций
GET    /functions/{id}     # метаданные функции
DELETE /functions/{id}     # удалить функцию
```

## Логи

```bash
sudo journalctl -u cloudfunctions-api -f
sudo journalctl -u cloudfunctions-executor -f
```

## Roadmap

- **Object Storage** — хранение кода функций в S3 вместо локального диска
- **Overlay FS** — кэш окружения без полного копирования при каждом вызове
- **Сеть** — доступ в интернет из функций
- **Rootless** — запуск без root
- **Метрики** — время выполнения, потребление памяти, интеграция с VK Cloud

## Требования к функциям

**Python** — загрузи `.py` файл, точка входа — запуск скрипта целиком.

**Go** — загрузи скомпилированный бинарь под `linux/amd64`.

**Java** — загрузи `.jar` файл, запускается через `java -jar`.