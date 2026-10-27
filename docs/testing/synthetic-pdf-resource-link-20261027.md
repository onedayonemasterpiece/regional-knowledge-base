# Disabled synthetic PDF ResourceLink probe

For a bounded client transport measurement only, the existing native full-profile
`fetch` recognizes `diagnostic:synthetic-pdf-resource-link:v1` when the service
environment contains exactly `RKB_DIAGNOSTIC_PDF_RESOURCE_LINK=1` at server creation.
The default is disabled; other IDs and ordinary fetch behavior remain unchanged.
The runtime owner controls temporary enable/disable through existing service
configuration after merge; this source change does not enable it.

The response contains a real MCP `ResourceLink` with `application/pdf`, size
13264 and the single fixed public URL:
`https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf`.
It also retains the existing fetch output schema (`structuredContent.result`
is a valid `FetchOutput`) and labels the resource as synthetic diagnostic material,
not source evidence. No caller URL, PDF contents, expected text or hash is accepted
or included. Existing destination-bound OAuth, `public_search` and fresh authority
recheck apply. There are no new grants or owner impersonation.
Issued/denied API log messages contain only fixed diagnostic purpose and status.

This probe reads no corpus/source, assembles no PDF and creates no compute demand.
Production returns only the public link and stores no PDF bytes on DevCoveer.
The in-process test uses the real MCP client/server and output validation with
synthetic OAuth/SQLite fixtures; it downloads no PDF. Actual ChatGPT/Slack link
opening and user-visible delivery are separate runtime-owner acceptance checks.
