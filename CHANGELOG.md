# Changelog

All notable changes to this module. Versions follow semantic versioning;
before v1 the API may change in minor versions.

## v0.1.0 (2026-10-11)

First release as a module of its own, split from the indiagov module.

- `pmkusum/sim`: an RMS of the "RMS Communication and Security
  Architecture" profile (2020 and 2022) on MQTT with sample messages of
  our own in every annexure 1 to 8 layout (including five USPC channels
  and a lower-case heartbeat with the other DATE form), OTP and info
  handling, annexure 7 answers, retrieval of stored records, persistent
  sessions, the pub/sub topic form and fault switches (held outputs,
  silence, lost, slow, duplicate and malformed answers, a wrong OTP); a
  vendor-style RMS.
- `jjm/sim`: a gateway and an HTTP device with messages of our own in
  the report's eight layouts (and a flow meter code for both error
  tables), a scheme RTU, a state-to-centre API and an eJalShakti portal
  (current APIs, retired and unlisted ones on request, JSON and XML),
  each with fault switches (refused keys, Status 0, server errors,
  outages, malformed answers, delays, bad requests).
- `cmd/kusumsim`, `cmd/jjmsim`: runners, with fault flags; jjmsim also
  serves the central API or the portal.
- A simulator's dial returns once its subscriptions are in place, so a
  command sent right after it is not lost.
- Tests at the module root drive the simulators with the indiagov
  library through an embedded broker.
- The documents' printed sample messages are not included.
