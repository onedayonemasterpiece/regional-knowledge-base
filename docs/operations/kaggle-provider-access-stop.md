# Stop provider polling after authentication or permission denial

The BGE controller exits with status 77 after the first HTTP 401 or 403 in
Dataset preflight, launch handling, or owned-version reconciliation. It keeps
the original queue and uncertain provider receipts. It does not create another
run or try another account to recover from denied access.

Before enabling this controller release, use the existing systemd unit's
drop-in mechanism:

```ini
[Service]
RestartPreventExitStatus=77
```

Place this in the existing controller unit's drop-in directory and run the
ordinary `systemctl --user daemon-reload`. No new service or timer is needed.
Verify `RestartPreventExitStatus` on the actual unit before starting it.
Otherwise `Restart=on-failure` would repeat the denied requests after each exit.

Changing a credential does not automatically resume the stopped controller.
Read the preserved operation state and validate the intended account before
the normal explicit restart. A host reboot may attempt the enabled unit once;
another denied response exits again without a restart loop. Disable the unit
as well when the account is deliberately suspended for investigation.

This change addresses the observed repeated probes after denied provider
access. Kaggle identified the earlier account restriction as a false positive;
this document does not attribute its moderation decision to those probes.
