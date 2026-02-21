# Postgres Type Mapping Matrix (Text Mode)

## Core types

| OID | Postgres type | `typoutput` sample | Go type from text decode (`DataType='t'`) | Notes |
| --- | --- | --- | --- | --- |
| 16 | `bool` | `t` | `bool` | Text accepts `t/true/TRUE`. |
| 17 | `bytea` | `\xdeadbeef` | `string` |  |
| 18 | `char` | `A` | `string` |  |
| 19 | `name` | `hello_name` | `string` |  |
| 20 | `int8` | `922337203685477580` | `int64` |  |
| 21 | `int2` | `32767` | `int64` | `strconv.ParseInt(..., 16)` returns `int64`. |
| 23 | `int4` | `2147483647` | `int64` | `strconv.ParseInt(..., 32)` returns `int64`. |
| 25 | `text` | `hello text` | `string` |  |
| 26 | `oid` | `12345` | `uint64` |  |
| 28 | `xid` | `42` | `uint64` |  |
| 29 | `cid` | `7` | `uint64` |  |
| 114 | `json` | `{"k":1,"arr":[1,2]}` | `json.RawMessage` | Preserved as raw JSON bytes. |
| 142 | `xml` | `<root a="1"/>` | `string` |  |
| 700 | `float4` | `123.25` | `float32` | `NaN/Infinity` returned as `string`. |
| 701 | `float8` | `123.25` | `float64` | `NaN/Infinity` returned as `string`. |
| 774 | `macaddr8` | `08:00:2b:ff:fe:01:02:03` | `string` |  |
| 790 | `money` | `$1,234.56` | `string` | Locale-dependent formatting. |
| 829 | `macaddr` | `08:00:2b:01:02:03` | `string` |  |
| 869 | `inet` | `192.168.1.10/24` | `string` |  |
| 1042 | `bpchar` | `ab  ` | `string` | Output keeps right-padding spaces. |
| 1043 | `varchar` | `varchar text` | `string` |  |
| 1082 | `date` | `2026-02-19` | `string` |  |
| 1083 | `time` | `04:05:06.123456` | `string` |  |
| 1114 | `timestamp` | `2026-02-19 04:05:06.123456` | `string` |  |
| 1184 | `timestamptz` | `2026-02-18 22:35:06.123456+00` | `string` | Probe session timezone was UTC. |
| 1186 | `interval` | `2 days 03:04:05.123456` | `string` |  |
| 1266 | `timetz` | `04:05:06.123456+07` | `string` |  |
| 1700 | `numeric` | `1234567890.123456789` | `string` | Preserves precision as text. |
| 2950 | `uuid` | `550e8400-e29b-41d4-a716-446655440000` | `string` |  |
| 3220 | `pg_lsn` | `0/16B6C50` | `string` |  |
| 3802 | `jsonb` | `{"k": 1, "arr": [1, 2]}` | `json.RawMessage` | `jsonb_out` normalizes spacing in text form. |

## Special float outputs (text mode)

| Type | `typoutput` | Go interpretation (`DataType='t'`) |
| --- | --- | --- |
| `float4` | `NaN` | `string` (`"NaN"`) |
| `float4` | `Infinity` | `string` (`"Infinity"`) |
| `float4` | `-Infinity` | `string` (`"-Infinity"`) |
| `float8` | `NaN` | `string` (`"NaN"`) |
| `float8` | `Infinity` | `string` (`"Infinity"`) |
| `float8` | `-Infinity` | `string` (`"-Infinity"`) |
