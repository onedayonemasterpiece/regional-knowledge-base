# Verified checkpoint restore versions

Binding a successor target can select a newer Dataset checkpoint before source
service composition. The selected checkpoint's identity does not prove that
the old source configuration's version pins describe those base bytes.

`_verified_restart_restore` therefore validates the cached manifest and refreshes
its base versions even when the selected checkpoint already equals the latest
verified checkpoint. Equal Dataset versions with different checkpoint identities
fail closed. Already correct configurations retain their original object identity.

The selected pgvector runtime pin remains unchanged; incompatible model,
tokenizer or native versions still fail. The base schema comes from the verified
manifest even when the template integer names a newer target schema. The target
database schema remains a separate native migration/readiness
requirement. No checkpoint bytes, retained restore plans or live state are rewritten.

Synthetic regression checks cover an already selected schema-44 base with stale
schema-42 template pins, both supported pgvector selections, and corrupt/missing
or differently bound manifests. Live recovery acceptance is separate from these
configuration tests and requires the existing exact restore authority.
