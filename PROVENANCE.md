# Provenance

## How the code was written

Written for SigmaM2M, a oneM2M platform, in Go, by the author of the
indiagov module and from the same public documents (indiagov's
PROVENANCE.md lists them with dates). No code was ported or copied from
another implementation. The simulators were first written next to
SigmaM2M's PM-KUSUM and JJM adapters to test them without devices,
moved into the indiagov module on 2026-10-06, and moved out of it into
this module on 2026-10-11 (with `git mv`, together with the runners and
the embedded test broker), so the library does not depend on its
simulators.

Added for this first release (2026-10-11): the RMS's fault switches
(`SetWrongOTP`, `SetAnswerDelay`, `SetDuplicateAnswers`,
`SetMalformedAnswers`, `DropNext`), retrieval of stored records
(`Store`), the pub/sub topic form (`DialOptions`), `PublishMessage`;
the central API's key check, outage, malformed answer, delay and method
and body checks; the portal's body reading through the library (JSON
and XML), unknown APIs, server errors, outage and delay; the gateway's
`PublishPayload`; the runners' heartbeat, fault and serve flags; the
simulators now return from their dial once their subscriptions are in
place, so a command sent right after it is not lost; and the tests at
the module root.

Where the documents do not say how a device or a server behaves, the
simulators' behaviour is our choice: answers on the command's topic,
LOAD 0 for a record not stored, 401, 404, 400 and 503 answers of the
central API (the report gives only Status 1 and 0), and the portal's
answers to an unknown API or a body it cannot read (the manuals show
only success and a wrong key). The vendor-style RMS and the scheme RTU
follow no document: no agency or vendor publishes such a format (F-2707,
F-488 and F-2706 in SigmaM2M's register; see below).

## Sources

| Area (files) | Followed |
|---|---|
| RMS (pmkusum/sim/sim.go, samples.go) | "RMS Communication and Security Architecture": PM KUSUM SEDM Platform (14/07/2020, published by MAHADISCOM as Annexure-H in a 2021 upload and as Annexure-I in a 2022 upload) and PM KUSUM National Portal (26/09/2022, published by OREDA as Annexure-E), sections 2 to 6 and annexures 1 to 8; field forms from public firmware (topic levels, lower-case keywords) |
| Vendor-style RMS (pmkusum/sim/vendor.go) | none (our own shape) |
| Gateway, HTTP device, central API (jjm/sim/jjmsim.go) | JJM Technical/Expert Committee report (March 2021), section 5(f) and annexure 9(c) |
| Scheme RTU (jjm/sim/rtu.go) | none (our own shape) |
| Portal (jjm/sim/ejalshakti.go) | eJalShakti operational manual (18 February 2026) and its 2021 edition (retired APIs), the help pages (read 2026-10-11) |
| Runners (cmd/kusumsim, cmd/jjmsim) | none (flags are our own) |

No document text is reproduced, and none of the documents' printed
sample messages is included. The sample messages here are our own, in
the layouts the documents describe, with made-up IMEIs, asset serials,
gids, values, hosts and credentials; every JJM value lies within the
report's reference ranges. The F-numbers in comments are F-472 (the
documents' samples carry a 16-digit IMEI, which our samples keep as a
form), F-2707 (no agency's RMS format besides this profile is public)
and F-488 (no device-facing format for JJM controllers is public); the
indiagov module's PROVENANCE.md explains each in full.

## Tests

| Test | Source |
|---|---|
| pmkusum_test.go, jjm_test.go (module root) | our own sample messages and the indiagov library against these simulators, through an embedded broker; same author |
| pmkusum/sim/sim_test.go | a fake MQTT dialer |
| cmd/kusumsim, cmd/jjmsim | the runners against an embedded broker and the library's clients |

All IMEIs, IDs, keys, passwords and host names are made-up demo values.

