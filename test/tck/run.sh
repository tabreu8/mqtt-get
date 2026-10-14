#!/usr/bin/env bash
# Runs the Eclipse Sparkplug TCK host application tests against mqtt-get
# (see tck_test.go). Needs Go, Java 11+ and Maven, and port 1883 free.
#
# The TCK (EPL-2.0) is downloaded from eclipse.org and checked against a
# pinned SHA-256; it is never committed to this repository. Set TCK_WORK
# to a directory to keep the download between runs.
set -euo pipefail
cd "$(dirname "$0")"

TCK_URL=https://download.eclipse.org/sparkplug/3.0.0/Eclipse-Sparkplug-TCK-3.0.0.zip
TCK_SHA256=a70b2c2f00d67ac714eadd5ac50f6241e0efa26036d95ca8ec667d491021b86b

work=${TCK_WORK:-$(mktemp -d)}
mkdir -p "$work"
zip=$work/Eclipse-Sparkplug-TCK-3.0.0.zip
if [ ! -f "$zip" ]; then
	curl -fsSL --retry 3 -o "$zip.part" "$TCK_URL"
	mv "$zip.part" "$zip"
fi
echo "$TCK_SHA256  $zip" | sha256sum -c -

run=$(mktemp -d "$work/run.XXXXXX")
unzip -q "$zip" 'SparkplugTCK/hivemq-extension/*' -d "$run"
mkdir -p "$run/extensions" "$run/data"
unzip -q "$run/SparkplugTCK/hivemq-extension/sparkplug-tck-3.0.0.zip" -d "$run/extensions"

mvn -q -B -f broker/pom.xml compile dependency:build-classpath -Dmdep.outputFile="$run/classpath"
# From $run: the TCK writes its result files to the working directory.
here=$PWD
(cd "$run" && exec java -cp "$here/broker/target/classes:$(cat classpath)" tck.Broker "$here/broker/conf" data extensions >broker.log 2>&1) &
broker=$!
trap 'kill $broker 2>/dev/null || true' EXIT
for _ in $(seq 120); do
	grep -q "BROKER READY" "$run/broker.log" && break
	kill -0 $broker 2>/dev/null || { cat "$run/broker.log"; exit 1; }
	sleep 0.5
done
grep -q "BROKER READY" "$run/broker.log" || { cat "$run/broker.log"; echo "broker did not start"; exit 1; }

status=0
SPARKPLUG_TCK_BROKER=tcp://127.0.0.1:1883 go test -count=1 -v -timeout 20m . || status=$?
if [ $status -ne 0 ]; then
	echo "--- broker log"
	cat "$run/broker.log"
fi
exit $status
