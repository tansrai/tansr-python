# Tansr Python SDK contributor guide

This repository contains the Python 3.7+ SDK and three public API demos under MIT.
Serve owns the agent core; device tools must not silently fall back to its host.

- Preserve frozen UAPI revision 7 bytes and generated operation/schema tables.
  This public distribution contains 20 approved contract assets. Use explicit
  --mode public with tools/contract_check.py and tools/generate_api.py --check.
  Never infer a different mode from missing assets.
- Run tools/check.py --contract-mode public --evidence <outside-source-directory>
  and supply --quality-python <modern-python> for the pinned lint/type tools.
  Public CI runs the common/public tests and builds both packages. The internal
  39-asset maintenance test is not part of this distribution.
- The seven integration drivers require an explicitly supplied compatible Serve
  fixture and provenance. No private Serve bundle or credentials are included;
  public CI does not claim that those separate integration gates ran.
- Keep Python 3.7 syntax, sync/async ownership, cancellation, authorization,
  original write identities, durable ACK recovery and material boundaries intact.
- Read the bilingual guides for installation, TLS runtime prerequisites and
  separate SDK/Demo packages. Source tests do not prove installed-only consumption.
- Do not commit credentials, build outputs, logs or user journals. A source export
  is a reviewed snapshot, not proof of a published tag or PyPI release.
