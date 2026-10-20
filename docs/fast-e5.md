# E5: retired target path

E5 removed from the owner-approved target runtime, storage, fallback, tokenizer dependencies and gates. Do not install, restart or backfill it. Preserve independently needed shared Python consumers and the pinned BGE tokenizer. BGE inference remains Kaggle-only.

Use the [single implementation handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) and [BGE boundary](operations/bge-kaggle-only.md). The [previous E5 document](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/fast-e5.md) and dated benchmark reports are historical evidence, not current deployment instructions.
