# Classic Text ML Example

This example is a design preview for dbt-ml's classic text and document ML
lane. It shows how a support-ticket corpus will flow from JSON extraction into
a planned TF-IDF feature model.

The project compiles today and emits `ml:` metadata into `manifest.json`.
Running the ML model requires the feature extractor and artifact lifecycle work
tracked in #40 and #44.

```bash
uv run dbt-ml --project-dir examples/classic_text_ml compile
```
