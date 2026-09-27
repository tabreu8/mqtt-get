# Broker interoperability suite

This suite runs mqtt-get end to end against real MQTT brokers, with every authentication method each broker supports. It starts a full mqtt-get server in-process and drives it only through its public HTTP API, while a separate MQTT client plays the role of a device.

## Last results

**320 checks passed, 0 failed, 1 skipped (a known broker limitation)** across 26 endpoints on 5 brokers. Run on 2026-09-27.

| Broker | Version | TCP | Password auth | WebSocket | TLS | mTLS | WSS | `$share` |
|---|---|---|---|---|---|---|---|---|
| Eclipse Mosquitto | 2.1.2 (also 2.0.18) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| EMQX | 6.3.1 | ✅ | ✅ | ✅ | ✅ | ✅ (+ password) | ✅ | ✅ |
| HiveMQ Community Edition | 2026.5 | ✅ | n/a¹ | ✅ | ✅ | ✅ | ✅ (mTLS) | ✅ |
| NanoMQ (`-full` image) | 0.25.6 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Coreflux MQTT Broker | 2.14.3 | ✅ | ✅ | ✅ | ✅ | ✅² | ✅ | ❌³ |
| mochi-mqtt (embedded in the unit tests) | 2.7.9 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | – |

¹ HiveMQ CE only ships an allow-all authentication extension, so passwords can't be enforced. TLS and mTLS are enforced.<br>
² Coreflux doesn't trust a client **CA**. It pins client **certificates**: put each client certificate as a `.pem` file in `ClientCertificateSourcePath`.<br>
³ Coreflux 2.14 rejects every `$share/...` subscription (SUBACK 0x80), even for a full-privilege user. Don't set `MQTT_SHARED_GROUP` with Coreflux. mqtt-get reports this clearly in `/api/v1/status`.

For every endpoint the suite checks the following. Checks that don't apply to an endpoint (for example certificate checks on plain TCP) are left out.

| Check | What it proves |
|---|---|
| `ingest_and_rest_get` | A device publishes → `GET /api/v1/values/...` returns it (JSON envelope and `?format=raw`) |
| `latest_value_wins` | 20 quick updates → GET returns the last one |
| `rest_publish_qos0/1/2` | `POST /api/v1/publish` at every QoS reaches the device |
| `rest_publish_batch` | 50 messages in one request, all delivered |
| `binary_payload` | Binary data survives both directions (raw body in, base64 out) |
| `retained_after_reconnect` | After clearing the store and reconnecting, the broker replays retained messages and they're flagged `retained: true` |
| `wildcard_query` | `?filter=.../+/temp` returns exactly the matching topics |
| `webhook` | A batched webhook receives exactly the matching messages |
| `wrong_password_rejected` | A bad password is refused and the error is reported |
| `no_client_cert_rejected` | An mTLS listener refuses a client with no certificate |
| `untrusted_ca_rejected` | Without the custom CA, the server certificate is refused |
| `shared_subscriptions` | 3 connections in a `$share` group: 300 messages ingested **exactly once** and spread over all connections |

Also verified by hand: when the broker is killed and restarted, mqtt-get reports 503 on `/healthz` with a clear error, refuses publishes with "not connected", then reconnects and resubscribes by itself about 1 s after the broker returns.

One incident during testing: NanoMQ 0.25.6 crashed once (`malloc(): unaligned fastbin chunk detected`, SIGABRT in its TLS transport) in 1 of about 11 runs. mqtt-get reported the lost connection. The same check passed in 6 targeted re-runs and 2 more full runs.

## Run it yourself

Requirements: Go 1.24+, Docker with Compose, and OpenSSL. Run from the repository root:

```bash
make interop-up      # generate test certificates and start all brokers
make interop         # run the suite (about 30 s)
make interop-down    # stop the brokers
```

Or step by step:

```bash
cd test/interop
./gen-certs.sh                       # throw-away CA, server, client certs and users
docker compose up -d                 # Mosquitto, EMQX, HiveMQ CE, NanoMQ x2, Coreflux x2
MQTT_INTEROP_CONFIG=$PWD/brokers.json go test -v -count=1 .
```

Useful variables:

| Variable | |
|---|---|
| `MQTT_INTEROP_CONFIG` | Path to the broker list. The suite is skipped when this is unset, so `go test ./...` stays hermetic. |
| `MQTT_INTEROP_ONLY` | Comma-separated broker names to test, e.g. `emqx,coreflux` |

Filter single endpoints with Go's `-run`, e.g. `-run 'TestInterop/hivemq-ce/mtls'`.

> The results above were produced with these exact config files and images. Docker wasn't available where the results were recorded, so each image's root filesystem was run with the same files bind-mounted at `/certs` and `/configs`. `docker-compose.yml` wires up those same mounts and ports but has not itself been run.

## Test your own broker

Write a JSON file in the same format as `brokers.json`. Relative paths are resolved against the file's directory.

```json
{
  "ca_file": "my-ca.pem",
  "client_cert_file": "device.crt",
  "client_key_file": "device.key",
  "brokers": [
    {"name": "production", "endpoints": [
      {"name": "tls", "url": "mqtts://broker.example.com:8883", "tls": "ca",
       "username": "svc", "password": "…", "wrong_password_rejected": true},
      {"name": "mtls", "url": "mqtts://broker.example.com:8884", "tls": "mtls",
       "no_client_cert_rejected": true, "shared_subscriptions": true}
    ]}
  ]
}
```

Endpoint fields:

| Field | |
|---|---|
| `url` | Any URL mqtt-get accepts: `tcp://`, `mqtt://`, `ssl://`, `tls://`, `mqtts://`, `ws://`, `wss://` |
| `username`, `password` | Credentials |
| `tls` | `""` (none), `"ca"` (verify the server with `ca_file`) or `"mtls"` (also send the client certificate) |
| `server_name` | TLS SNI / verification name override |
| `wrong_password_rejected`, `no_client_cert_rejected`, `shared_subscriptions` | Turn on the optional checks |
| `skip` | `{"check_name": "reason"}` to skip a check that the broker is known not to support |

All test traffic stays under `mginterop/<endpoint>/<random>/...`, and the retained test message is cleared afterwards, so it's safe to run against a shared broker. It does create a few short-lived client connections.

## Broker configuration notes

These notes come from getting each broker running. See `configs/` for the exact files.

- **Mosquitto**: one `listener` block per auth method with `per_listener_settings true`. The password file must be hashed with `mosquitto_passwd`. `use_identity_as_username true` makes the certificate CN the username on the mTLS listener.
- **EMQX**: everything is configured with `EMQX_*` environment variables (`configs/emqx.env`). A second SSL listener named `mtls` sets `verify_peer` and `fail_if_no_peer_cert`. Users come from a bootstrap CSV for the built-in database. Authentication is global, so the mTLS endpoint also sends the password.
- **HiveMQ CE**: TLS uses a PKCS#12 keystore; mTLS also needs a truststore and `client-authentication-mode REQUIRED`. `gen-certs.sh` builds `trust.p12` with `keytool`, OpenSSL ≥ 3.2, or the HiveMQ image's `keytool`.
- **NanoMQ**: use the `emqx/nanomq:*-full` image, because the default image is built **without TLS** (`nng_listener_create tls: Not supported`). Enabling `listeners.ssl` also starts a WSS listener on a default port, so set `listeners.wss` explicitly. mTLS and plain TLS need separate instances (one `listeners.ssl` per process).
- **Coreflux**: configured with `--config file.json --config-force` and `--users users.json --users-force`. It loaded our PEM certificate as if it were a PFX and ignored the separate key file, so use a password-protected **PFX** (`server.pfx`). `MaxPendingMessagesPerClient` must be ≤ 100000. For mTLS, see note ² above. Only one instance per host can use the ops port 9100 (`COREFLUX_OPS_PORT`).
