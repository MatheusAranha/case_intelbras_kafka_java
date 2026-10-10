# Technical Decisions & Trade-offs (Bloco C)

| Decision | Alternative considered | Why this way |
|---|---|---|
| `EventCategory` derived from the real `msgType` field | A fabricated `device_type` column (the original assumption, before the real CSV arrived) | The real data has no such column; `msgType` is the only always-present categorical field and maps directly onto the case brief's own four event kinds |
| 4 topics, grouping `msgType`'s 11 raw values | One topic per raw `msgType` | 11 topics for very uneven volumes (1 row up to 45 rows) fragments operations for no benefit at this scale |
| `timestamp_log` as the single timestamp source, parsed via `OffsetDateTime` | The nested `utcTime`/`time` fields inside `body_completo` | Those vary format (epoch seconds vs. compact strings) by message family; `timestamp_log` is uniform across all 100 rows |
| Native id (`body_completo.id`/`alarmId`) preferred over hash, when present | Always hash | Real native ids exist for 26/100 rows, verified unique; more faithful to the source system |
| Blank `did` → fallback to `body_completo.deviceId` → `"UNKNOWN"` | Reject the row at ingestion | Demonstrates RT04 tolerance on real, mildly inconsistent data instead of dropping 2 real rows |
| DLQ demo via 4 appended synthetic rows, clearly labeled | Force a failure into a real row; cover DLQ only in the integration test | The case explicitly allows adding events beyond the original 100; a labeled synthetic row keeps the real dataset unmodified and still demonstrates retry/DLQ live via `docker compose` |
| Two independent domains (`device`, `audit`) | One domain covering ingestion+processing+audit | Audit is one of potentially several independent consumers of the event stream (e.g. a future `notification` domain) |
| Ports as Java interfaces, standard Spring autowiring | Abstract classes; manual composition root | Idiomatic Java/Spring; same dependency-inversion guarantee |
| Duplicates/failures: log only, no audit row, no `status` field | Persist `DUPLICATE`/`FAILED` rows | The audit table should only ever reflect successfully processed events |
| PostgreSQL for the audit store | H2 | Real persistence across restarts; works with Testcontainers and the Docker Compose bonus |
| One structured `ExecutionLog` per behaviour | Scattered `logger.info` calls | Every behaviour's outcome is a single, greppable, structured line |

## Known limitations

- No authentication/authorization on the REST endpoints — out of scope for the case.
- DLT replay is manual, by design.
- A frontend for browsing `/audits` is a planned separate project, not part of this submission.
