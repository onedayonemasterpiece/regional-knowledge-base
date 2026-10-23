# Correcting inactive native source input

After a rejected seal, the calling model can append a corrective stage batch using the same chunk key or an explicit replacement entity with the same identity and kind. Accepted batch bytes and their hash chain remain immutable. The compiler selects the latest explicitly replaced chunk before compiling source selections, then validates graph evidence against the final corrected source before tokenization.

An unrelated chunk key cannot suppress an invalid selection. Missing evidence, changed entity identity, unknown model fields, incomplete coverage and oversized complete encoder inputs still reject. The original rejected seal receipt remains in history; the corrected head needs its own successful seal. This does not relax native SQL activation checks or Dataset durability.

Focused verification: `tests/test_native_source_materializer.py` covers invalid footnote selection followed by an explicit correction, a boundary-whitespace graph quote correction, unresolved evidence, unrelated keys and identity changes. Native deterministic materialization rejections log only route, sequence and a bounded validation code.
