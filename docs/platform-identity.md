# One person across product resources

Active contract2026-10-20. [Auth](auth.md) and the [single handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) define implementation and acceptance.

One canonical platform/RKB person UUID is linked to verified provider+tenant+subject accounts. Products retain their own resource-specific grants, domain ownership and revocation. Display names/email are profile fields; professional weight and bibliographic author identity are not authentication.

Personal MCP, Slack and Mira route to that same person, fair quota and request history. An executor service is recorded separately from the human author. Wrong binding triggers attribution review. Unlinking an account stops future access without pretending past correctly attributed contributions never existed.

Do not build another IAM or expert account registry. Reuse the existing application identity/consent machinery and verified bindings; converging product sign-in must not require corpus reindexing. A token for another product is never accepted/forwarded as an RKB grant. Same-host machine integrations use narrow dedicated identities, not user OAuth or owner impersonation.

GEO-only content migration does not remove existing users, bindings, permissions, integration configuration or secrets. Public text/illustration policy does not expose a person's requests, private drafts or proof scans. Verify real multi-user route/revocation isolation; the former one-owner beta is historical, not the target user model.
