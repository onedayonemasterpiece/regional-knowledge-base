# Rights and visibility model

This system must distinguish “can search privately” from “may publish to everyone”.

## Default

Every upload starts:
- source: private;
- content: private;
- rights status: unknown.

The model may extract bibliographic facts and propose a rights hypothesis, but it cannot promote visibility from its own assertion.

## Rights status

```text
unknown
restricted
licensed
permission_granted
public_domain_candidate
public_domain_verified
```

A document can be public only when the rights status is one of:
- licensed;
- permission_granted;
- public_domain_verified.

This rule belongs in the database as well as application code.

## No “75 years old = public” shortcut

Publication age alone is insufficient.

For a named Russian author, the normal rule is life + 70 years, with statutory special cases including an additional four years for authors who worked or participated during the Great Patriotic War, plus rules for rehabilitation and posthumous publication.

German law normally uses life + 70 for identified authors; anonymous/pseudonymous works have separate rules, and scientific editions / first publication of posthumous works can carry related protection.

Therefore a pre-war German book or a book older than 75 years is only a **candidate for rights verification**, not automatically public.

## Work, edition, scan and media are separate

Even when the underlying text is public domain:
- a modern scientific edition may have separate protection;
- photographs/illustrations may have their own author and term;
- a modern scan may carry contractual/access constraints;
- a user's private copy may contain annotations or personal information.

The rights record therefore tracks work-level, edition-level and media/source-level evidence separately.

## Verification evidence

A public-domain verification record should contain, as applicable:
- jurisdiction/policy version;
- author identity;
- death year and evidence URL/reference;
- publication year;
- anonymous/pseudonymous status;
- relevant special-case flags and whether they are known;
- edition/publication provenance;
- evidence references;
- verifier type and timestamp.

Unknown material facts fail closed.

## Automatic promotion

Automatic promotion is allowed only when a deterministic policy can prove every required fact from trusted evidence. Otherwise the service may mark `public_domain_candidate` and request review.

Never expose the private source PDF merely because normalized content is promoted public.
