# Migration function pins

`rkb-migration-pins` measures actual PostgreSQL 17 / pgvector 0.8.2
`pg_get_functiondef` UTF-8 SHA-256 values on a fresh GitHub fixture. It installs
SQL001–011, records old functions, then SQL012, SQL013/014 and the existing
`transition_sql()` definitions. It verifies every old function is unchanged.
It does not apply accepted product commands or verify runtime migration success.

Before using the artifact, compare every configured live schema11 handler check
and apply definition with the corresponding measured function. Require exact
equality and matching source SQL bytes; any mismatch blocks use of the new pins.
Keep live configuration, database identities, endpoints and credentials local.

The GEO function-pin block deliberately omits `origin_catalog_sha256` and
`origin_acl_sha256`: obtain these from the independently verified live origin.
The fixture's PostgreSQL owner/ACL/catalog cannot stand in for product roles.
The native source block is the measured `NativeSourceUpgradePins` wire shape.
Review the artifact and exact transition path before live use. This job grants
no deployment, provider push, Dataset write or production database permission.
