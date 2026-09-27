#!/usr/bin/env bash
# Generates a throw-away PKI for the interop tests into ./certs:
#   ca.crt/ca.key          test certificate authority
#   server.crt/server.key  broker certificate (localhost, 127.0.0.1 + broker hostnames)
#   server.pfx             same, PKCS#12 (password "changeit") for Coreflux / HiveMQ
#   trust.p12              CA as a Java truststore (password "changeit") for HiveMQ
#   client.crt/client.key  client certificate for mutual TLS (CN=mqtt-get-client)
#   clients/client.pem     client certificate for Coreflux (pins client certs)
#   users.csv / pwd.conf / users.json  user "mg" / "mg-pass" for EMQX / NanoMQ / Coreflux
# NEVER use these files in production.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p certs/clients
cd certs
PASS=changeit
SAN="subjectAltName=DNS:localhost,IP:127.0.0.1,DNS:mosquitto,DNS:emqx,DNS:hivemq,DNS:nanomq-a,DNS:nanomq-b,DNS:coreflux-a,DNS:coreflux-b"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
  -keyout ca.key -out ca.crt -subj "/CN=mqtt-get interop test CA" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout server.key -out server.csr -subj "/CN=localhost" 2>/dev/null
printf '%s\nextendedKeyUsage=serverAuth\n' "$SAN" > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
  -out server.crt -extfile server.ext 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=mqtt-get-client" 2>/dev/null
printf 'extendedKeyUsage=clientAuth\n' > client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
  -out client.crt -extfile client.ext 2>/dev/null

openssl pkcs12 -export -out server.pfx -inkey server.key -in server.crt -certfile ca.crt -passout pass:$PASS
# Java truststore for HiveMQ: keytool, else OpenSSL >= 3.2, else the HiveMQ image's keytool.
rm -f trust.p12
if command -v keytool >/dev/null 2>&1; then
  keytool -importcert -noprompt -alias ca -file ca.crt -keystore trust.p12 -storetype PKCS12 -storepass $PASS >/dev/null 2>&1
elif openssl pkcs12 -help 2>&1 | grep -q jdktrust; then
  openssl pkcs12 -export -nokeys -in ca.crt -out trust.p12 -jdktrust anyExtendedKeyUsage -passout pass:$PASS
else
  docker run --rm -v "$PWD:/c" --entrypoint keytool hivemq/hivemq-ce -importcert -noprompt -alias ca \
    -file /c/ca.crt -keystore /c/trust.p12 -storetype PKCS12 -storepass $PASS >/dev/null
fi
cp client.crt clients/client.pem

printf 'user_id,password,is_superuser\nmg,mg-pass,true\n' > users.csv
printf 'mg: mg-pass\n' > pwd.conf
cat > users.json <<'JSON'
[{"UserName":"mg","Password":"mg-pass","AllowedBaseTopic":"#","AllowedSystemConfiguration":false,"AllowedUserManagement":false,"AllowedLogManagement":false}]
JSON
# Mosquitto needs a hashed password file.
rm -f mosquitto.passwd
if command -v mosquitto_passwd >/dev/null 2>&1; then
  mosquitto_passwd -b -c mosquitto.passwd mg mg-pass 2>/dev/null
else
  docker run --rm -v "$PWD:/c" eclipse-mosquitto:2 mosquitto_passwd -b -c /c/mosquitto.passwd mg mg-pass
fi
rm -f ./*.csr ./*.ext ./*.srl
# Brokers run as unprivileged users inside their containers.
chmod 644 ./* clients/*
echo "certificates written to $(pwd)"
