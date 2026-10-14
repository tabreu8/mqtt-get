"""A Sparkplug B edge node built on pysparkplug (an independent Python
implementation of Sparkplug B), driven by interop_test.go.

Commands arrive as JSON lines on stdin; events go to stdout as JSON lines.

    python edge_node.py <host> <port> <group> <edge_node>

Commands: {"cmd":"update","metric":..,"value":..}, {"cmd":"update_alias_only",..},
{"cmd":"kill"} (exit without DISCONNECT: the broker publishes the NDEATH will),
{"cmd":"disconnect"} (clean disconnect with an NDEATH).
"""
import datetime
import itertools
import json
import os
import sys
import threading

import pysparkplug as psp
from pysparkplug import DataType as T

host, port, group, edge = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
lock = threading.Lock()


def emit(**event):
    with lock:
        sys.stdout.write(json.dumps(event) + "\n")
        sys.stdout.flush()


def now():
    return psp.get_current_timestamp()


STARTED = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)

# Node metrics: one of each scalar and array type, with the values the Go
# test expects (see interop_test.go).
NODE = [
    psp.Metric(timestamp=now(), name="Node Control/Rebirth", datatype=T.BOOLEAN, value=False),
    psp.Metric(timestamp=now(), name="Types/Int8", datatype=T.INT8, value=-8),
    psp.Metric(timestamp=now(), name="Types/Int16", datatype=T.INT16, value=-16000),
    psp.Metric(timestamp=now(), name="Types/Int32", datatype=T.INT32, value=-2000000000),
    psp.Metric(timestamp=now(), name="Types/Int64", datatype=T.INT64, value=-9000000000000000000),
    psp.Metric(timestamp=now(), name="Types/UInt8", datatype=T.UINT8, value=250),
    psp.Metric(timestamp=now(), name="Types/UInt16", datatype=T.UINT16, value=65000),
    psp.Metric(timestamp=now(), name="Types/UInt32", datatype=T.UINT32, value=4000000000),
    psp.Metric(timestamp=now(), name="Types/UInt64", datatype=T.UINT64, value=18000000000000000000),
    psp.Metric(timestamp=now(), name="Types/Float", datatype=T.FLOAT, value=1.5),
    psp.Metric(timestamp=now(), name="Types/Double", datatype=T.DOUBLE, value=-2.125),
    psp.Metric(timestamp=now(), name="Types/Boolean", datatype=T.BOOLEAN, value=True),
    psp.Metric(timestamp=now(), name="Types/String", datatype=T.STRING, value='quote " and ünïcode'),
    psp.Metric(timestamp=now(), name="Types/Text", datatype=T.TEXT, value="multi\nline"),
    psp.Metric(timestamp=now(), name="Types/UUID", datatype=T.UUID, value="123e4567-e89b-12d3-a456-426614174000"),
    psp.Metric(timestamp=now(), name="Types/DateTime", datatype=T.DATETIME, value=STARTED),
    psp.Metric(timestamp=now(), name="Types/Bytes", datatype=T.BYTES, value=b"\x00\x01\xfe\xff"),
    psp.Metric(timestamp=now(), name="Types/Null", datatype=T.DOUBLE, value=None, is_null=True),
    psp.Metric(timestamp=now(), name="Arrays/Int8", datatype=T.INT8_ARRAY, value=(-128, 0, 127)),
    psp.Metric(timestamp=now(), name="Arrays/Int16", datatype=T.INT16_ARRAY, value=(-32768, 32767)),
    psp.Metric(timestamp=now(), name="Arrays/Int32", datatype=T.INT32_ARRAY, value=(-1, 2, -3)),
    psp.Metric(timestamp=now(), name="Arrays/Int64", datatype=T.INT64_ARRAY, value=(-5000000000, 5000000000)),
    psp.Metric(timestamp=now(), name="Arrays/UInt8", datatype=T.UINT8_ARRAY, value=(0, 255)),
    psp.Metric(timestamp=now(), name="Arrays/UInt16", datatype=T.UINT16_ARRAY, value=(1, 65535)),
    psp.Metric(timestamp=now(), name="Arrays/UInt32", datatype=T.UINT32_ARRAY, value=(4294967295,)),
    psp.Metric(timestamp=now(), name="Arrays/UInt64", datatype=T.UINT64_ARRAY, value=(18446744073709551615,)),
    psp.Metric(timestamp=now(), name="Arrays/Float", datatype=T.FLOAT_ARRAY, value=(0.5, -1.25)),
    psp.Metric(timestamp=now(), name="Arrays/Double", datatype=T.DOUBLE_ARRAY, value=(3.5, -0.001)),
    psp.Metric(timestamp=now(), name="Arrays/Boolean", datatype=T.BOOLEAN_ARRAY, value=(True, False, True, True, False, False, False, False, True)),
    psp.Metric(timestamp=now(), name="Arrays/String", datatype=T.STRING_ARRAY, value=("a", "", "c d")),
    psp.Metric(timestamp=now(), name="Arrays/DateTime", datatype=T.DATETIME_ARRAY, value=(STARTED,)),
]

DEVICE = [
    psp.Metric(timestamp=now(), name="Temperature", datatype=T.DOUBLE, value=20.0),
    psp.Metric(timestamp=now(), name="Setpoint", datatype=T.FLOAT, value=50.0),
    psp.Metric(timestamp=now(), name="Offset", datatype=T.INT32, value=0),
    psp.Metric(timestamp=now(), name="Mode", datatype=T.STRING, value="auto"),
    psp.Metric(timestamp=now(), name="Running", datatype=T.BOOLEAN, value=False),
]
DATATYPES = {m.name: m.datatype for m in NODE + DEVICE}


def json_value(v):
    if isinstance(v, (bytes, bytearray)):
        return list(v)
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    if isinstance(v, tuple):
        return [json_value(x) for x in v]
    return v


def device_cmd(node, message):
    # A DCMD written by mqtt-get, decoded by pysparkplug: report it, then
    # apply it and confirm with a DDATA, like a real device.
    metrics = message.payload.metrics
    emit(event="cmd", topic=str(message.topic),
         metrics=[{"name": m.name, "datatype": int(m.datatype), "value": json_value(m.value)} for m in metrics])
    node.update_device("press", [psp.Metric(timestamp=now(), name=m.name, datatype=m.datatype, value=m.value) for m in metrics])


births = 0


def node_cmd(node, message):
    global births
    metrics = message.payload.metrics
    emit(event="ncmd", topic=str(message.topic),
         metrics=[{"name": m.name, "datatype": int(m.datatype), "value": json_value(m.value)} for m in metrics])
    for m in metrics:
        if m.name == "Node Control/Rebirth" and m.value:
            rebirth(node)


# pysparkplug prepares the will (and its bdSeq) for the *next* connection as
# soon as it has published a birth, so remember the bdSeq of the session in
# progress to publish rebirths with it.
session_bd = []
_setup_will = psp.EdgeNode._setup_will


def setup_will(self):
    _setup_will(self)
    session_bd.append(self._bd_seq_metric)


psp.EdgeNode._setup_will = setup_will


def rebirth(node):
    global births
    bd = session_bd[-2]  # the will registered for this connection
    client = node._client
    # A birth restarts the sequence at 0 (Sparkplug 3.0).
    node._EdgeNode__seq_cycler = itertools.cycle(range(256))
    client.publish(psp.Message(
        topic=psp.Topic(message_type=psp.MessageType.NBIRTH, group_id=group, edge_node_id=edge),
        payload=psp.NBirth(timestamp=now(), seq=node._seq, metrics=(*node.metrics.values(), bd)),
        qos=psp.QoS.AT_MOST_ONCE, retain=False), include_dtypes=True)
    for dev in node.devices.values():
        client.publish(psp.Message(
            topic=psp.Topic(message_type=psp.MessageType.DBIRTH, group_id=group, edge_node_id=edge, device_id=dev.device_id),
            payload=psp.DBirth(timestamp=now(), seq=node._seq, metrics=tuple(dev.metrics.values())),
            qos=psp.QoS.AT_MOST_ONCE, retain=False), include_dtypes=True)
    births += 1
    emit(event="rebirth", births=births, bd_seq=bd.value)


node = psp.EdgeNode(group, edge, NODE, client=psp.Client(client_id="pysparkplug-" + edge), cmd_callback=node_cmd)
node.register(psp.Device("press", DEVICE, cmd_callback=device_cmd))
node.connect(host, port=port, blocking=False)


def wait_connected():
    import time
    # Connected, and the birth callback has finished (it prepares the will
    # of the next session last).
    for _ in range(100):
        if node._connected and len(session_bd) >= 2:
            return
        time.sleep(0.05)
    emit(event="error", error="not connected")
    os._exit(1)


wait_connected()
births = 1
emit(event="born", bd_seq=session_bd[-2].value, library="pysparkplug", version=psp.__version__ if hasattr(psp, "__version__") else "")

for line in sys.stdin:
    cmd = json.loads(line)
    if cmd["cmd"] == "update":
        name, value = cmd["metric"], cmd["value"]
        m = psp.Metric(timestamp=now(), name=name, datatype=DATATYPES[name], value=value)
        if name in {d.name for d in DEVICE}:
            node.update_device("press", [m])
        else:
            node.update([m])
        emit(event="updated", metric=name)
    elif cmd["cmd"] == "kill":
        emit(event="killing")
        os._exit(0)  # no DISCONNECT: the broker publishes the will (NDEATH)
    elif cmd["cmd"] == "disconnect":
        node.disconnect()
        emit(event="disconnected")
        break
