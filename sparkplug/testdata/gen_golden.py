"""Regenerates the golden payloads of codec_test.go (birthHex, dataHex,
metadataHex, extensionsHex)
with the reference protobuf implementation and the official Eclipse Tahu
sparkplug_b.proto (as generated in pysparkplug):

    python3 -m venv /tmp/spb && /tmp/spb/bin/pip install pysparkplug==0.6.1
    /tmp/spb/bin/python sparkplug/testdata/gen_golden.py
"""
import json
import struct

from pysparkplug._protobuf import sparkplug_b_pb2 as pb


def birth():
    p = pb.Payload(timestamp=1760000000123, seq=0)
    p.metrics.add(name="bdSeq", datatype=8, long_value=7, timestamp=1760000000123)
    p.metrics.add(name="Node Control/Rebirth", alias=1, datatype=11, boolean_value=False)
    m = p.metrics.add(name="Temperature", alias=2, datatype=9, float_value=21.5, timestamp=1760000000100)
    m.properties.keys.extend(["engUnit", "engHigh"])
    m.properties.values.add(type=12, string_value="°C")
    m.properties.values.add(type=10, double_value=150.0)
    p.metrics.add(name="Count", alias=3, datatype=3, int_value=(-5) & 0xFFFFFFFF)
    p.metrics.add(name="Small", alias=4, datatype=1, int_value=(-2) & 0xFFFFFFFF)
    p.metrics.add(name="Big", alias=5, datatype=4, long_value=(-9000000000) & 0xFFFFFFFFFFFFFFFF)
    p.metrics.add(name="U64", alias=6, datatype=8, long_value=18446744073709551615)
    p.metrics.add(name="Pressure", alias=7, datatype=10, double_value=142.7)
    p.metrics.add(name="Running", alias=8, datatype=11, boolean_value=True)
    p.metrics.add(name="Mode", alias=9, datatype=12, string_value='auto "fast"')
    p.metrics.add(name="Started", alias=10, datatype=13, long_value=1760000000000)
    p.metrics.add(name="Blob", alias=11, datatype=17, bytes_value=b"\x00\x01\xff")
    p.metrics.add(name="Nothing", alias=12, datatype=9, is_null=True)
    p.metrics.add(name="Ints", alias=13, datatype=24, bytes_value=struct.pack("<3i", -1, 2, 300000))
    p.metrics.add(name="Floats", alias=14, datatype=30, bytes_value=struct.pack("<2f", 1.5, -2.25))
    p.metrics.add(name="Bools", alias=15, datatype=32, bytes_value=struct.pack("<I", 3) + bytes([0b10100000]))
    p.metrics.add(name="Names", alias=16, datatype=33, bytes_value=b"ab\x00c\x00")
    d = p.metrics.add(name="Table", alias=17, datatype=16)
    d.dataset_value.num_of_columns = 2
    d.dataset_value.columns.extend(["id", "label"])
    d.dataset_value.types.extend([3, 12])
    r = d.dataset_value.rows.add()
    r.elements.add(int_value=(-1) & 0xFFFFFFFF)
    r.elements.add(string_value="x")
    t = p.metrics.add(name="Motor", alias=18, datatype=19)
    t.template_value.template_ref = "MotorType"
    t.template_value.metrics.add(name="rpm", datatype=7, int_value=1500)
    t.template_value.parameters.add(name="maxRpm", type=7, int_value=3000)
    p.metrics.add(name="Folder/Deep/Value", alias=19, datatype=7, int_value=4000000000)
    return p


def data():
    p = pb.Payload(timestamp=1760000001000, seq=1)
    p.metrics.add(alias=2, timestamp=1760000001000, float_value=22.25)
    p.metrics.add(alias=3, int_value=(-6) & 0xFFFFFFFF)
    p.metrics.add(alias=99, int_value=1)  # unknown alias
    return p


def metadata():
    p = pb.Payload(timestamp=1760000002000, seq=2)
    m = p.metrics.add(name="Config", alias=20, datatype=18, bytes_value=b"PK\x03\x04")
    m.metadata.is_multi_part = True
    m.metadata.content_type = "application/zip"
    m.metadata.size = 4096
    m.metadata.seq = 3
    m.metadata.file_name = "config.zip"
    m.metadata.file_type = "zip"
    m.metadata.md5 = "d41d8cd98f00b204e9800998ecf8427e"
    m.metadata.description = "part 3"
    return p


def with_unknown(msg, raw):
    """Returns msg with the encoded fields raw added as unknown fields."""
    out = type(msg)()
    out.ParseFromString(msg.SerializeToString() + raw)
    return out


def extensions():
    # Extension values and fields unknown to the schema (field 100 as a
    # varint, field 101 as bytes) at every level that keeps them.
    unk = bytes([0xa0, 0x06, 0x07, 0xaa, 0x06, 0x02]) + b"hi"
    ext = pb.Payload.Metric.MetricValueExtension()
    ext.ParseFromString(bytes([0x08, 0x2a, 0x12, 0x03]) + b"ext")
    p = pb.Payload(timestamp=1760000003000, seq=3)
    m = pb.Payload.Metric(name="Custom", alias=21, datatype=12)
    m.extension_value.CopyFrom(ext)
    m.metadata.CopyFrom(with_unknown(pb.Payload.MetaData(content_type="x/y"), unk))
    pv = pb.Payload.PropertyValue(type=12)
    pv.extension_value.ParseFromString(bytes([0x08, 0x01]))
    ps = pb.Payload.PropertySet(keys=["custom", "unit"])
    ps.values.append(pv)
    ps.values.append(with_unknown(pb.Payload.PropertyValue(type=12, string_value="m"), unk))
    m.properties.CopyFrom(with_unknown(ps, unk))
    p.metrics.append(with_unknown(m, unk))
    d = pb.Payload.Metric(name="Table", alias=22, datatype=16)
    ds = pb.Payload.DataSet(num_of_columns=1, columns=["c"], types=[12])
    cell = ds.rows.add().elements.add()
    cell.extension_value.ParseFromString(bytes([0x08, 0x02]))
    d.dataset_value.CopyFrom(with_unknown(ds, unk))
    p.metrics.append(d)
    t = pb.Payload.Metric(name="Motor", alias=23, datatype=19)
    tpl = pb.Payload.Template(template_ref="MotorType")
    par = pb.Payload.Template.Parameter(name="custom", type=12)
    par.extension_value.ParseFromString(bytes([0x08, 0x03]))
    tpl.parameters.append(with_unknown(par, unk))
    t.template_value.CopyFrom(with_unknown(tpl, unk))
    p.metrics.append(t)
    return with_unknown(p, unk)


print(json.dumps({"birth": birth().SerializeToString().hex(), "data": data().SerializeToString().hex(),
                  "metadata": metadata().SerializeToString().hex(),
                  "extensions": extensions().SerializeToString().hex()}))
