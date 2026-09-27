# mqtt-get

A small, fast MQTT ↔ HTTP bridge written in Go. It is one static binary (about 8 MB, about 16 MB RSS with 10k topics) that:

- **connects to any MQTT broker** and keeps the **latest value of every topic** in memory
- **REST `GET`** returns the most recent value of a topic, or of every topic matching a wildcard filter
- **REST `POST`** publishes to MQTT (one message or a batch)
- **Webhooks** forward matching messages to HTTP endpoints, with batching, retries, HMAC signatures and per-webhook backpressure
- **API-key auth** with scopes (`read`, `publish`, `admin`)
- **Every common MQTT auth method**: anonymous, username/password or token, password file, TLS with a custom CA, mutual TLS (client certificates), SNI, ALPN, and WebSocket headers
- **Several ways to configure it**: environment variables, REST API, **MCP** (for AI agents), or the built-in **web UI**
- **Prometheus metrics**, a health check and optional pprof

## Quick start

```bash
# Docker Compose (includes a Mosquitto broker)
docker compose up -d
open http://localhost:8080        # API key: change-me-admin-key

# or a plain binary
make build
MQTT_URL=tcp://broker.local:1883 API_KEYS=my-secret-key ./bin/mqtt-get
```

If you start it with no API keys, it **generates an admin key and prints it once in the log**. You can also start with no broker configured and set it up later in the web UI.

```bash
KEY=my-secret-key
# latest value (JSON envelope)
curl -H "Authorization: Bearer $KEY" localhost:8080/api/v1/values/home/kitchen/temperature
# {"topic":"home/kitchen/temperature","payload":{"celsius":21.4},"encoding":"json","qos":0,"retained":false,"timestamp":"2026-09-27T17:52:30.19Z"}

# raw payload only
curl -H "Authorization: Bearer $KEY" "localhost:8080/api/v1/values/home/kitchen/temperature?format=raw"

# publish
curl -X POST -H "Authorization: Bearer $KEY" --data ON "localhost:8080/api/v1/publish/home/lamp/set?qos=1"
```

## REST API

Authenticate with `Authorization: Bearer <key>` or `X-API-Key: <key>` (plus `?api_key=` if `ALLOW_QUERY_API_KEY=true`).

| Method & path | Scope | Description |
|---|---|---|
| `GET /api/v1/values/{topic...}` | read | Latest value of a topic. `?format=raw` returns only the payload. `?max_age=30s` returns 404 if the value is older than that. Metadata is also sent in `X-MQTT-*` headers. |
| `GET /api/v1/values?topic=a/b` | read | Same, for topics that don't fit in a URL path (for example, a leading `/` or `//`). |
| `GET /api/v1/values?filter=home/%2B/temp&limit=1000` | read | Latest values of all topics matching an MQTT filter. URL-encode `+` as `%2B` and `#` as `%23`. |
| `GET /api/v1/topics?filter=…` | read | Topic names, payload sizes and timestamps. |
| `DELETE /api/v1/values/{topic...}`, `DELETE /api/v1/values` | admin | Forget one topic, or all of them. |
| `POST /api/v1/publish/{topic...}?qos=1&retain=true` | publish | The request body is the raw payload. |
| `POST /api/v1/publish` | publish | JSON: `{"topic","payload","qos","retain","encoding"}` or an **array** of them. The messages are pipelined. A JSON string payload is sent as its text, and any other JSON value as its JSON encoding. Use `"encoding":"base64"` for binary. |
| `GET/POST /api/v1/webhooks`, `GET/PUT/DELETE /api/v1/webhooks/{id}`, `POST /api/v1/webhooks/{id}/test` | admin | Manage webhooks. Responses include delivery stats. |
| `GET/PUT /api/v1/broker`, `POST /api/v1/broker/reconnect` | admin | View or change the broker connection. |
| `GET/POST /api/v1/keys`, `DELETE /api/v1/keys/{id}` | admin | Manage API keys. Only a SHA-256 hash of each key is stored. |
| `GET /api/v1/status`, `GET /api/v1/whoami` | read / any | Status and the current key's identity. |
| `GET /metrics` | read (or public with `METRICS_PUBLIC=true`) | Prometheus metrics. |
| `GET /healthz` | none | Returns 200 when healthy, or 503 when a broker is configured but disconnected. |
| `POST /mcp` | per tool | MCP endpoint (see below). |

Secrets (passwords, private keys, webhook secrets, auth headers) come back as `********` in API responses. If you send `********` back in an update, the stored value is kept, so you can GET, edit and PUT safely.

Payload `encoding` in responses is `json` (embedded as-is), `utf8` (a string) or `base64` (binary).

## Webhooks

```bash
curl -X POST -H "Authorization: Bearer $KEY" localhost:8080/api/v1/webhooks -d '{
  "name": "alerts",
  "url": "https://example.com/hook",
  "topics": ["alarms/#", "home/+/smoke"],
  "secret": "optional-hmac-secret",
  "batch_size": 100, "batch_interval_ms": 200,
  "headers": {"Authorization": "Bearer downstream-token"}
}'
```

- `format: "json"` (default) sends the same envelope as the REST API. With `batch_size > 1` the body is `{"messages":[…]}`, flushed when the batch is full or after `batch_interval_ms`.
- `format: "raw"` sends the payload as the body, with `X-MQTT-Topic`, `X-MQTT-QoS`, `X-MQTT-Retained` and `X-MQTT-Timestamp` headers.
- **Signature**: when `secret` is set, requests carry `X-MQTT-Get-Timestamp: <unix>` and `X-MQTT-Get-Signature: sha256=HEX(HMAC_SHA256(secret, timestamp + "." + body))`.
- **Retries**: network errors, 5xx, 408 and 429 are retried with exponential backoff (`max_retries`, default 3).
- **Backpressure**: each webhook has its own bounded queue (`queue_size`, default 10000) and `concurrency` workers. A slow endpoint never slows down ingestion or other webhooks; when its queue is full, messages for that webhook are dropped and counted in `mqttget_webhook_dropped_total`.
- Topic matching uses an immutable trie, so adding many webhooks doesn't slow down the hot path.

## MQTT authentication and connection options

| Method | How |
|---|---|
| Anonymous | Just `MQTT_URL` |
| Username / password, API tokens, JWT-as-password (for example, Azure SAS, or Google/AWS custom auth) | `MQTT_USERNAME`, `MQTT_PASSWORD` |
| Rotating tokens or Docker secrets | `MQTT_PASSWORD_FILE`. The file is re-read on every reconnect. |
| TLS (server verification) | `ssl://`, `mqtts://` or `wss://` URL, or `MQTT_TLS=true`. Custom CA via `MQTT_TLS_CA_FILE` or `MQTT_TLS_CA_PEM`. |
| Mutual TLS / client certificates (for example, AWS IoT Core) | `MQTT_TLS_CERT_FILE` + `MQTT_TLS_KEY_FILE` (or `_PEM`) |
| SNI / ALPN (for example, AWS IoT on port 443) | `MQTT_TLS_SERVER_NAME`, `MQTT_TLS_ALPN=x-amzn-mqtt-ca` |
| Self-signed certificates (testing only) | `MQTT_TLS_INSECURE=true` |
| MQTT over WebSockets, including auth headers | `ws://` or `wss://` URL + `MQTT_WS_HEADERS="Authorization: Bearer x"` |
| Unix socket | `unix:///path/to/sock` |
| Failover | Several URLs, comma-separated |

Supported protocols are MQTT 3.1.1 (default) and 3.1. Default ports are 1883 and 8883. MQTT 5-only features such as enhanced (SASL-style) AUTH are not supported.

## Configuration (environment variables)

| Variable | Default | |
|---|---|---|
| `MQTT_URL` | – | Broker URL(s), comma-separated |
| `MQTT_TOPICS` | `#` | Filters to subscribe to, comma-separated, each with an optional `:qos` suffix, e.g. `home/#:1,sensors/+` |
| `MQTT_QOS` | `0` | Default subscription QoS |
| `MQTT_CLIENT_ID` | `mqtt-get-<hostname>` | |
| `MQTT_USERNAME`, `MQTT_PASSWORD`, `MQTT_PASSWORD_FILE` | | |
| `MQTT_TLS`, `MQTT_TLS_CA_FILE`/`_PEM`, `MQTT_TLS_CERT_FILE`/`_PEM`, `MQTT_TLS_KEY_FILE`/`_PEM`, `MQTT_TLS_SERVER_NAME`, `MQTT_TLS_INSECURE`, `MQTT_TLS_ALPN` | | TLS options |
| `MQTT_WS_HEADERS` | | `Name: value`, comma-separated |
| `MQTT_PROTOCOL_VERSION` | `4` | `4` = 3.1.1, `3` = 3.1 |
| `MQTT_CLEAN_SESSION` | `true` | |
| `MQTT_KEEPALIVE_SEC`, `MQTT_CONNECT_TIMEOUT_SEC` | `30`, `10` | |
| `MQTT_CONNECTIONS`, `MQTT_SHARED_GROUP` | `1`, – | Scale-out (see below) |
| `API_KEYS` | – | Admin keys, comma-separated |
| `API_KEYS_READ`, `API_KEYS_PUBLISH` | – | Read-only keys; read + publish keys |
| `AUTH_DISABLED` | `false` | Turns off auth (only on trusted networks) |
| `HTTP_ADDR` | `:8080` | |
| `HTTP_TLS_CERT_FILE`, `HTTP_TLS_KEY_FILE` | – | Serve HTTPS |
| `DATA_DIR` | `./data` | Where `state.json` (broker settings, webhooks, API key hashes) is saved |
| `MAX_TOPICS` | `1000000` | Cap on stored topics, to protect memory |
| `MAX_BODY_BYTES` | `1048576` | |
| `PUBLISH_TIMEOUT_MS` | `5000` | |
| `UI_ENABLED`, `MCP_ENABLED`, `METRICS_PUBLIC` | `true`, `true`, `false` | |
| `CORS_ORIGINS` | – | e.g. `*` or `https://app.example.com` |
| `ALLOW_QUERY_API_KEY` | `false` | |
| `LOG_LEVEL`, `LOG_FORMAT` | `info`, `text` | `json` is also available for the format |
| `PPROF_ADDR` | – | e.g. `127.0.0.1:6060` |

**Precedence:** broker settings saved through the UI, API or MCP are stored in `DATA_DIR/state.json` and take precedence over `MQTT_*` variables. Delete that `broker` entry (or the file) to go back to the environment. The dashboard shows which source is active.

## MCP (AI agents)

`POST /mcp` implements the Model Context Protocol (Streamable HTTP, JSON responses). The tools are `get_latest_value`, `query_values`, `list_topics`, `publish`, `get_status`, `get_broker_config`, `configure_broker`, `list_webhooks`, `create_webhook`, `update_webhook`, `delete_webhook`, `test_webhook` and `create_api_key`. Each key only sees the tools its scopes allow.

```bash
# Claude Code
claude mcp add --transport http mqtt http://localhost:8080/mcp --header "Authorization: Bearer $KEY"
```

For stdio-only clients such as Claude Desktop, use the built-in bridge:

```json
{"mcpServers": {"mqtt": {"command": "mqtt-get", "args": ["mcp", "--url", "http://localhost:8080"],
                         "env": {"MQTT_GET_API_KEY": "…"}}}}
```

## Performance and scaling

Design choices for the hot path:

- Messages are handled on the connection's receive goroutine, with no goroutine per message.
- The socket reads are buffered. The MQTT client library reads packet headers byte by byte; a 64 KiB read buffer saves about 30% CPU per message.
- The latest-value store is sharded across 256 locks. Entries are immutable and shared with webhook queues without copying.
- Webhook routing uses a lock-free trie that is swapped atomically when webhooks change.

Measured on a 4-vCPU VM with Mosquitto, the load generator and mqtt-get all on the same machine:

| | Result |
|---|---|
| Ingest cost inside mqtt-get (store + webhook routing) | ~270 ns/message |
| End-to-end, one connection, 64-byte payloads, 10k topics | ~225k msg/s with no loss. Mosquitto was the bottleneck; mqtt-get used ~4.3 µs of CPU per message, mostly in the MQTT client library. |
| Memory | ~16 MB RSS with 10k topics |

To go past one connection, set `MQTT_CONNECTIONS=N` and `MQTT_SHARED_GROUP=name`. mqtt-get then opens N connections that subscribe with `$share/name/<filter>`, and the broker load-balances messages across them, so ingest uses N cores. The store keeps the newest value per topic even if the connections deliver out of order. Without a shared group, the extra connections are only used to spread publish load.

For millions of messages per second, run several instances, each subscribed to a slice of the topic tree (`MQTT_TOPICS`), and route HTTP by topic prefix. Or run one instance per shared-subscription group member when you only need webhooks.

Benchmark your own setup:

```bash
go run ./cmd/loadgen -url tcp://broker:1883 -clients 4 -n 2000000 -topics 10000
curl -s -H "Authorization: Bearer $KEY" localhost:8080/metrics | grep mqttget_messages_received_total
make bench   # micro-benchmarks
```

## Development

```bash
make test    # go vet + race-enabled tests; the integration tests use an embedded broker
make build   # bin/mqtt-get
```

Layout: `cmd/mqtt-get` (entry point, stdio MCP bridge, healthcheck), `cmd/loadgen`, and under `internal/`: `mqttc` (broker connections and auth), `store` (latest values), `topic` (matching and trie), `webhook`, `state` (persistence and API keys), `config`, `api` (REST, MCP, metrics, embedded UI).
