# Dated evidence and reuse map

Audited 2026-10-20. These pinned repository records are not fresh deployment checks. Use the incidents to select tests, not to assert today's runtime repeats them.

## events-bot-new

Source revision: `d431b9f86b87999cf27f74b99857d23aedc28f16`.

- [June 13 duplicate publication](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-06-13-kaggle-duplicate-videoannounce.md#L13-L40): session 662 published, later final output download failed; replacement 664 duplicated publication. Recover result delivery, not the committed effect.
- [April 16 false SaveKernel success](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-04-16-cherryflash-kaggle-save-kernel-drift.md#L14-L43): quota error was logged as deployment success; old notebook remained. Check returned error/version and applied identity.
- [June 10 retry storm](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-06-10-guide-watchdog-kaggle-failure-loop.md#L37-L85): versions 291–317, in-memory guards and early ownership gaps failed across restarts. Persist intent before submit and reconcile terminal registry records.
- [June 26 missing artifact](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/docs/reports/incidents/INC-2026-06-26-crumple-missing-video-output.md#L14-L79): COMPLETE/render_done did not establish final mp4; false success followed a font failure.
- Reuse [durable launch intent and conservative unknown-push reconciliation](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/kaggle_registry.py#L133-L394), and [event dedup/heartbeat](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/kaggle_status.py#L703-L837). These do not supply bounded log retention or a complete generic WSS updater.
- [Status/output wrapper](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/video_announce/kaggle_client.py#L1457-L1542) is slug-scoped; dependency was `kaggle>=1.8.2`, not an installed-version receipt. Do not assume latest version-qualified behavior.
- Do not copy [cancel-state conflation](https://github.com/onedayonemasterpiece/events-bot-new/blob/d431b9f86b87999cf27f74b99857d23aedc28f16/video_announce/poller.py#L2519-L2548) as proof of stopped execution: requested/acknowledged cancellation is not terminal death. Dataset deletion used against queued zombies is likewise not stop proof.

## my-data-hub

Source revision: `a26797759fe30718fc6a12206c344968dcdc80a9`.

- [Migration runner](https://github.com/onedayonemasterpiece/my-data-hub/blob/a26797759fe30718fc6a12206c344968dcdc80a9/src/my_data_hub/db/migrations.py) supplies immutable filename/hash and transactional SQL+ledger+schema revision. It is not a complete RKB live migration/replay runner: add only needed serialization, prefix validation, nontransactional steps and replay ordering.
- [Exact result reconciliation](https://github.com/onedayonemasterpiece/my-data-hub/blob/a26797759fe30718fc6a12206c344968dcdc80a9/src/my_data_hub/providers/kaggle/master_runtime.py#L1289-L1348) validates run/attempt/source/epoch and executed-source attestation. It protects result acceptance, not provider old-session status resolution.
- [Adapter source identity](https://github.com/onedayonemasterpiece/my-data-hub/blob/a26797759fe30718fc6a12206c344968dcdc80a9/src/my_data_hub/providers/kaggle/adapter.py#L1744-L1838) and selective output use current-source checks, while attested-master paths deliberately use runtime attestation instead. Do not claim every status read is exact-session fenced.
- [FM08 deletion](https://github.com/onedayonemasterpiece/my-data-hub/blob/a26797759fe30718fc6a12206c344968dcdc80a9/src/my_data_hub/providers/kaggle/adapter.py#L1840-L1907) verifies resource absence after kernels_delete, not independent compute-session death. Its one call per invocation still needs caller fencing across retries.
- Package pin was `kaggle==2.2.4`; historical isolated validation recorded `kagglesdk==0.1.37`. Neither proves a currently deployed package. The runtime SDK's event spool/replay is useful; callback-loss lane records are offline fault tests, not necessarily live incidents.

## Upstream capability boundary

[Kaggle issue 1169](https://github.com/Kaggle/kaggle-cli/issues/1169) reports session-ID/cancel difficulties with 2.2.4. It is a reported observation, not a permanent platform guarantee. An external collaborator reported current CLI source `6894864ff4095cf98e9e36d3a694ff5fc9ba3818` passing `version_label` and internally following output pages. That report was not independently revalidated when this skill was authored. Inspect the selected installed package before relying on either behavior. Do not add a manual paginator merely because a wrapper has no visible loop.
