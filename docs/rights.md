# Rights, collection provenance and visibility

The system must distinguish four different questions:

1. who owns or controls the physical/source copy;
2. whether the service may ingest and privately search it;
3. whether normalized content may be shared with a workspace;
4. whether text or media may be distributed publicly.

Those are not the same legal fact.

## Safe default

Every user upload starts:

- source visibility: `private`;
- content visibility: `private`;
- media visibility: `private`;
- rights status: `unknown`.

A model may extract bibliographic facts and suggest a rights hypothesis. A model
cannot by itself make a private upload public.

## Verified statuses

```text
unknown
restricted
licensed
permission_granted
public_domain_candidate
public_domain_verified
statutory_access_verified
```

`statutory_access_verified` exists for collections where public use follows
from a separately reviewed legal/statutory/institutional basis rather than the
ordinary copyright-term calculation.

Public visibility requires **all three**:

1. a verified status: `licensed`, `permission_granted`,
   `public_domain_verified` or `statutory_access_verified`;
2. a non-empty `rights_policy_version`;
3. evidence explicitly containing `public_distribution: true`.

The database enforces this again; UI/model instructions are not the security
boundary.

## Pre-war German / East Prussian historical collections

Regional collections can contain books and other cultural values moved from
Germany after World War II under the compensatory-restitution regime. Russian
law and Constitutional Court practice recognize a specific property regime for
lawfully displaced cultural values and, for qualifying former-enemy-state
property, federal ownership / Russian cultural-property status.

That provenance is important and must be represented directly. Do **not** force
such works through the generic rule “publication year + N years”.

Recommended evidence shape:

```json
{
  "basis_type": "compensatory_restitution_collection",
  "collection_id": "stable-institution-or-fund-id",
  "provenance_verified": true,
  "property_basis_verified": true,
  "public_distribution": true,
  "evidence_refs": ["..."],
  "verified_by": "operator-or-institution",
  "verified_at": "..."
}
```

A trusted collection policy can map a verified provenance to
`statutory_access_verified`. This allows automatic availability for a known,
reviewed corpus without asking the model to re-litigate every volume.

The implementation still stores physical/source ownership separately from
copyright/publication rights. This is deliberate: the federal law on displaced
cultural values is a property regime, while copyright protection for foreign
works is governed separately. If the collection's reviewed policy establishes
the required public-use basis, the service uses that policy; it does not infer it
from “German”, “pre-war”, age, or a model assertion alone.

## Ordinary copyright-term triage

For material outside a trusted collection policy, publication age alone remains
insufficient. Russian and foreign works may depend on author death, anonymous or
pseudonymous publication, rehabilitation, posthumous publication, war-related
term extensions, international treaties and country-of-origin rules.

The deterministic age/death calculator therefore produces at most
`public_domain_candidate`. Promotion to a verified status requires evidence.

## Work, edition, source and illustration are separate

Even when a work is publicly usable:

- a later edition can have separate rights;
- photographs and illustrations can have their own authors/bases;
- a private scan can contain personal annotations or marginalia;
- an institution can permit access to normalized content while keeping exact
  source files private.

For this reason the raw uploaded PDF remains private by default even when its
normalized content becomes public. Illustrations have their own visibility and
rights evidence.

## Policy-driven automation

Automatic promotion is allowed only from a versioned trusted policy plus
deterministic evidence. A useful policy registry entry contains:

- policy ID/version;
- collection/source identifiers it applies to;
- allowed content/media actions;
- required provenance fields;
- legal/institutional evidence references;
- effective/review dates;
- whether public distribution is permitted.

Unknown or conflicting facts fail closed to private. Changing a policy never
requires re-OCR or rechunking: only rights/visibility projections are re-evaluated.
