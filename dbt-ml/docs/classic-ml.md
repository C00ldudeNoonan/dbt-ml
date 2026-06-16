# Classic ML Text and Document Models

dbt-ml should treat classic text and document ML as a primary lane, not just a
fallback for LLM or RAG work. Count vectors, TF-IDF, hashing features,
classifiers, clustering, topic models, named-entity enrichment, deduplication,
and deterministic metrics are cheaper, easier to reproduce, and often enough
for production document pipelines.

This page defines the v0.2 design contract. The `ml:` block is parsed and
emitted into artifacts now; executors land incrementally in follow-up issues
such as #40 and #44.

## Model Shape

Classic ML models use the same model list and DAG rules as extraction and
Python transform models. They declare dependencies with `depends_on` and use an
`ml:` block for task, mode, provider, artifact, metrics, and provider options.

```yaml
models:
  - name: ticket_tfidf
    depends_on: [ref('raw_tickets')]
    ml:
      task: features
      mode: fit_transform
      provider: builtin.tfidf
      text_field: body
      artifact:
        path: target/artifacts/ticket_tfidf
      metrics: [row_count, vocabulary_size]
      options:
        ngram_range: [1, 2]
        min_df: 3
        max_features: 50000
```

## Tasks

The first task set is intentionally small and maps to stable production
patterns:

| task | Purpose | Likely first providers |
| --- | --- | --- |
| `features` | Count, TF-IDF, hashing, dense/sparse document features | `builtin.count`, `builtin.tfidf`, `builtin.hashing` |
| `classifier` | Supervised document or row classification | `sklearn.logistic_regression`, `sklearn.linear_svc`, `sklearn.sgd_classifier` |
| `regressor` | Supervised numeric prediction | `sklearn.linear_model`, `sklearn.ensemble` |
| `cluster` | Document clustering and nearest-neighbor grouping | `sklearn.kmeans`, later HDBSCAN-style providers |
| `topic_model` | Topic discovery and document-topic tables | `sklearn.nmf`, `sklearn.lda` |
| `nlp` | Tokenization, POS, NER, sentiment, key phrases | `spacy`, lightweight built-ins |

Providers should remain optional dependencies. The base package should keep
working for extraction and pure-Python transforms without installing
scikit-learn or spaCy.

## Modes

The same grammar needs to support training and applying artifacts:

| mode | Meaning |
| --- | --- |
| `fit_transform` | Fit from upstream rows and materialize transformed output in one run. |
| `fit` | Fit and persist an artifact, optionally emitting metadata/metrics only. |
| `predict` | Load an artifact and materialize predictions/features for incoming rows. |
| `load_pretrained` | Register or apply an externally trained artifact without fitting it. |

## Artifact Contract

Artifacts should live under `target/artifacts/<model_name>/` by default unless
the user provides `ml.artifact.path`. A provider may write multiple files
there, but it must expose a stable metadata record:

```json
{
  "model_name": "ticket_tfidf",
  "task": "features",
  "provider": "builtin.tfidf",
  "mode": "fit_transform",
  "artifact_version": "16-char-hash",
  "training_input": {
    "refs": ["raw_tickets"],
    "row_count": 1000,
    "content_hash": "16-char-hash"
  },
  "metrics": {
    "vocabulary_size": 12000
  },
  "files": ["vocabulary.json", "metadata.json"]
}
```

`manifest.json` should contain the static `ml:` config and `code_version`.
`run_results.json` should eventually include artifact version, training input,
and metrics for executed ML models. The first executor should keep this shape
small rather than inventing per-provider result formats.

## Versioning

Classic ML versioning should account for:

- the `ml:` config block;
- provider name and provider options;
- project-local code if a custom provider is used;
- upstream training data hash or declared artifact hash;
- dependency/provider version when it affects output.

This branch adds the `ml:` block to `code_version`, which covers static config.
Training-data and artifact hashes belong with the executor work.

## Initial Examples

`examples/classic_text_ml/` is a design-preview project showing a support-ticket
feature extraction pipeline using `ml.task: features` and `provider:
builtin.tfidf`. It is intended to compile and emit manifest metadata; execution
requires the #40 feature extractor and #44 artifact lifecycle work.
