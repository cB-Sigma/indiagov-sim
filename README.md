# indiagov-sim

Simulated PM-KUSUM RMS units, Jal Jeevan Mission gateways and RTUs, a state-to-centre API and an eJalShakti portal, with fault switches, for testing without field devices; not affiliated with any government body.

[![Go Reference](https://pkg.go.dev/badge/github.com/cB-Sigma/indiagov-sim.svg)](https://pkg.go.dev/github.com/cB-Sigma/indiagov-sim) [![test](https://github.com/cB-Sigma/indiagov-sim/actions/workflows/test.yml/badge.svg)](https://github.com/cB-Sigma/indiagov-sim/actions/workflows/test.yml) [![Licence: Apache-2.0](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE) [![Go](https://img.shields.io/badge/go-1.24.0-00ADD8.svg)](go.mod)

The RMS speaks the message profile of "RMS Communication and Security
Architecture" (the PM KUSUM SEDM Platform version of 14/07/2020,
published by MAHADISCOM as Annexure-I, and the PM KUSUM National Portal
version of 26/09/2022, published by OREDA as Annexure-E) over MQTT: it
publishes sample messages of our own in every annexure layout (or
messages written with the library),
carries the OTP it last received, keeps the info message, and answers
on-demand and config reads and writes the way annexure 7 describes,
including the 0 answer to an unknown keyword. The JJM gateway publishes
messages of our own in the report's device layouts; the central API and the portal take pushes
and answer as the documents show. Every simulator has switches for the
faults a client must survive. It is built on
[indiagov](https://github.com/cB-Sigma/indiagov). Written for SigmaM2M,
a oneM2M platform; it has no dependency on it.

## What is simulated

| Simulator | Behaviour | Faults |
|---|---|---|
| PM-KUSUM RMS (`pmkusum/sim.RMS`) | samples of our own in the annexure 1 to 8 layouts, five USPC channels and a lower-case heartbeat with the YYYY-MM-DD DATE (`Publish`), library-written messages (`PublishMessage`), raw bytes (`PublishRaw`); takes the OTP pair and carries it; keeps the info message; answers on-demand and config reads and writes on the command's topic, unknown keywords with 0; replays stored records for `pmkusum.Retrieve` (`Store`); persistent sessions that queue commands while offline (`DialSession`, `Offline`, `Online`); the pub/sub topic form of field firmware (`DialOptions`) | held outputs (`Hold`), silence (`Silence`), lost commands (`DropNext`), slow answers (`SetAnswerDelay`), duplicate answers (`SetDuplicateAnswers`), answers that are not JSON (`SetMalformedAnswers`), a wrong OTP (`SetWrongOTP`), malformed messages (`PublishRaw`), the RMS down (`Offline`), refused broker credentials (the broker's) |
| Vendor-style RMS (`pmkusum/sim.VendorRMS`) | a format no agency publishes, in the shape vendor RMS units have: nested telemetry, a backlog, commands acknowledged on their own topic | local mode refusing pump commands, bad values, unknown operations, silence |
| JJM gateway (`jjm/sim.Gateway`) | messages of our own in the report's eight layouts and a flow meter code for both error tables, library-written messages (`PublishPayload`), any bytes | malformed messages |
| JJM HTTP device (`jjm/sim.Post`) | posts a message to an API method with a key | |
| Scheme RTU (`jjm/sim.RTU`) | a vendor-style controller (no format is published): pump, valve and interval commands, readings, persistent sessions | local mode, bad arguments, unknown commands, silence, offline with queued commands |
| State-to-centre API (`jjm/sim.Central`) | annexure 9(c)(iii): POST {Name}DataInsert, answered with Status, Message, DeviceID and UploadedOn; records what it took | Status 0 (`FailNext`), a required key (`SetKey`: 401), down (`SetDown`: 503), answers that are not JSON (`SetMalformed`), delays (`SetDelay`), unknown methods (404), bodies that are not device messages or do not match the method (400) |
| eJalShakti portal (`jjm/sim.Portal`) | the five current APIs at /IoT_API/API/{API} (the retired GroundWater and Pressure and the unlisted water quality APIs with `AcceptRetired`), JSON and DataContract XML bodies read as the portal reads them, a key per API in Authorization, Status true with "sucess" | a wrong key answered Status true with the not-matched message, renewed keys (`SetKey`), server errors (`FailNext`: 500), down (`SetDown`: 503), delays (`SetDelay`), unknown APIs (404), bodies it cannot read (400) |
| A real controller's timing, GSM coverage, the agencies' own servers | no | |

## Install

```
go get github.com/cB-Sigma/indiagov-sim@latest
```

Requires Go 1.24.0 or later. The commands:

```
go install github.com/cB-Sigma/indiagov-sim/cmd/kusumsim@latest
go install github.com/cB-Sigma/indiagov-sim/cmd/jjmsim@latest
```

## Command line

```
kusumsim -broker mqtt://127.0.0.1:1883 -imei 860000000000001 -solution DEMO -every 1m
jjmsim -broker mqtt://127.0.0.1:1883 -topic jjm/MH_D02_B03_V04 -every 15m
jjmsim -serve portal -key demo-key
```

| kusumsim flag | Default | Meaning |
|---|---|---|
| `-broker` | `mqtt://127.0.0.1:1883` | MQTT broker URL (`mqtt://user:pass@host:port`, `mqtts://` for TLS) |
| `-imei`, `-solution` | `860000000000001`, `DEMO` | the RMS's IMEI and the topics' solution level |
| `-every`, `-count` | `15m`, `0` | push interval; stop after this many data messages (0: never) |
| `-heartbeat` | `true` | send the heartbeat with each data message |
| `-session` | `0` | keep a persistent broker session this long |
| `-pubsub` | `false` | the field topic form with a sub or pub fifth level |
| `-silent`, `-drop`, `-delay`, `-duplicate`, `-malformed`, `-wrong-otp`, `-hold` | off | fault switches (see the table above; `-hold DO1` keeps the pump as it is) |

| jjmsim flag | Default | Meaning |
|---|---|---|
| `-broker`, `-topic` | `mqtt://127.0.0.1:1883`, `jjm/MH_D02_B03_V04` | where the device samples go |
| `-every`, `-rounds` | `15m`, `0` | interval between rounds of the eight samples; stop after this many (0: never) |
| `-serve` | | `central` or `portal`: serve that API on a local port instead (the URL is printed) |
| `-key` | | the key the served API requires |
| `-fail`, `-down`, `-malformed` | off | fault switches of the served API |

## In Go tests

```go
r, _ := sim.Dial(ctx, brokerURL, "860000000000001", "Standalonesolarpump") // pmkusum/sim
r.Hold("DO1", true)                                  // the pump stays as it is
_ = r.Publish(ctx, "data", sim.SamplePump, nil)      // annexure 1 sample, with the RMS's IMEI and OTP
// ... your platform sends pmkusum.Pump("", "41", time.Now(), true) and reads the answer:
// pmkusum.Evaluate(cmd, answer).NotApplied == []string{"DO1"}

portal := jjmsim.NewPortal(map[string]string{"Chlorine": "demo-key"}) // jjm/sim
defer portal.Close()
c := jjm.EJClient{BaseURL: portal.URL + "/IoT_API/API", Key: "stale-key"}
_, err := c.Push(ctx, "Chlorine", body)              // jjm.ErrKeyRefused
```

## Documentation

Reference: https://pkg.go.dev/github.com/cB-Sigma/indiagov-sim

| Package | What it does |
|---|---|
| `pmkusum/sim` | `RMS` (`Dial`, `DialSession`, `DialOptions`; `Publish`, `PublishMessage`, `PublishRaw`, `Store`, `Set`, `Get`, `Hold`, the fault switches, `Offline`, `Online`, `OTP`, `Info`, `Commands`), `VendorRMS`, sample messages of our own in the annexure layouts (`SamplePump`, `SampleUSPC`, `SampleMeter`, `SampleInverter`, `SampleSJB`, `SampleHeartbeat`, `SampleHeartbeatLower`, `SampleDAQ`, `SampleConfigWrite`, `SampleInvalidKeyCommand`, `SampleInvalidKeyResponse`), `DialMQTT` (the MQTT seam) |
| `jjm/sim` | `Gateway`, `Post`, `RTU`, `Central`, `Portal`, `PostEj`, sample messages in the report's layouts (`Samples`, `SampleFlowmeterSection5`, `SampleAPIResponse`) and eJalShakti bodies (`EjChlorine`, `EjPH`, `EjFlowMeter`, `EjGroundWater`, `EjChlorineXML`, `EjPHXML`, `EjPressure`), `DialMQTT` |
| `cmd/kusumsim`, `cmd/jjmsim` | the runners above |

Changes per version are in [CHANGELOG.md](CHANGELOG.md).

## Testing

```
go test -race ./...
```

The tests at the module root start an embedded MQTT broker and drive
the simulators with the indiagov library: every sample message of the
documents over MQTT and through the decoders, the OTP flow and a wrong
OTP, every annexure 7 action, each fault switch, retrieval, a queued
command after a reconnection, the pub/sub topic form, a broker refusing
credentials, and the library's state-to-centre and eJalShakti clients
against the simulated APIs (success, retries, refused keys, outages,
bad requests). Tests may carry `// Covers: SIG-...` comments:
requirement IDs of the SigmaM2M project's compliance report, with no
meaning outside it. Code comments that cite `F-` numbers are explained
in PROVENANCE.md.

### Interop tested against

Not against an independent implementation: none exists in the open
(see indiagov's README), and no agency platform, eJalShakti portal or
field RMS was available. The evidence is messages of our own built to
the documents' layouts (the documents' printed samples are not
shipped), which the simulators send and the library's readers take,
and the library's clients run against the simulated APIs. The simulators
and the indiagov library have the same author and follow the same
documents, so their agreement is not independent.

## Demo values

Every IMEI, gid, device ID, village ID, gateway ID, key, password and
host name in this module is a made-up demo value. Do not use them for real devices.

## Related repositories

- [indiagov](https://github.com/cB-Sigma/indiagov): the message
  readers, writers and clients these simulators are built on.

## Status

v0.x: the API may change before v1.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md) to report a vulnerability privately.

## Licence

Apache-2.0 (see [LICENSE](LICENSE) and [NOTICE](NOTICE)). Copyright 2026
Abhinav Gautam. PM-KUSUM, Jal Jeevan Mission and eJalShakti are names of
Government of India schemes and services, used only to say what this
module simulates; it is not made, endorsed or supported by any
ministry, agency or utility.
