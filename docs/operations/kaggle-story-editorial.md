# Native shared Story editorial operations

This in-progress schema009 unit preserves existing `story_edit`/`UpsertVariant`
and manager-only `story_access` semantics. It does not create a private Story
variant API. Personal drafts remain the existing `disagreement_conclusion`
operation group and require independent native case authority before enabling.

Native per-Story grants retain viewer/contributor/researcher/editor/publisher/
manager roles. The imported Story manager is its exact accepted import author;
a global category or supplied UUID confers no editorial grant. Native CAS checks
the actual Story and Variant revisions, active source revision and eligibility.
The controller must bind actor_id to the authenticated accepted command person
and recheck current canonical Participants authority; the SQL argument is not a
credential. Direct public SQL or an owner fallback is not supported.

Each saved Variant retains its exact body and immutable version. Editing the
current default Variant advances the exact default reference and indexed body;
new alternate variants remain separately addressable. A save clears publication
approval. Reading shared editorial material does not grant publication rights.
Source withdrawal suppresses current and historical reads. Old001..008 SQL and
accepted command revisions remain unchanged; fresh009 has a separate importv4
binding. Nonempty schema008 adoption/latest-first replay is not supported by
this unit and still requires the original ordered migration barrier.

Three native PG17.10/pgvector0.8.2 synthetic tests passed in5.746seconds with a
192MiB/96MiB-PGDATA disposable fixture, followed by stop/remove/absence proof.
They cover editor grants and manager-only changes, exact body/default reference,
retry/new-connection/history, unauthorized or revoked editor rejection, source
withdrawal, projection failure rollback and original-order exact outcome replay.
These are local native function results, not a live target/MCP deployment.
The native9 intake, output guard, complete history proof, bounded reads for all
metadata boundary cases, observed catalogue/ACL pin and actual WSS composition
remain in progress. No public profile or live runtime is changed.
