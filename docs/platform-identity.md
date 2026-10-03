# Platform identity: one user, many MCP products

## Principle

A single person may use Regional Knowledge, Wonderful Lections, Street Story,
Projects Hub and other MCPs, but **Supabase is not the common identity provider**.

The shared concept is a stable platform user UUID plus resource-specific OAuth
grants.

```text
                 application/platform identity
                       stable user UUID
                             |
        +--------------------+--------------------+
        |                    |                    |
 Regional Knowledge   Wonderful Lections    Projects Hub
   OAuth resource       OAuth resource       OAuth resource
        |                    |                    |
   own ACL/RLS            own ACL             own ACL
```

## Beta vs platform convergence

Do not build a new identity platform before the Knowledge beta works.

For the first Knowledge beta:
- one owner account;
- application-owned OAuth 2.1;
- stable UUID subject;
- exact Knowledge resource binding.

Wonderful Lections already provides a proven OAuth behavior donor. It remains
operational and unchanged.

After the beta, extract/converge identity so multiple MCPs can use the same
platform user UUID and sign-in session. That later convergence must not require
changing RKB document owners or reindexing corpus data.

## Resource isolation

Every MCP remains a different OAuth resource.

A token for Wonderful Lections is not accepted by Knowledge and vice versa.
Cross-service access uses an explicit target-resource grant, never bearer-token
forwarding.

## User experience target

Eventually:
1. user signs in once to the common platform identity;
2. each MCP asks only for its own consent;
3. the same platform user UUID is used everywhere;
4. grants can be revoked independently.

This platform target is independent of the database vendor used by any product.
