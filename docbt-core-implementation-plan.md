# docbt — Core Implementation Plan

> **Date:** March 27, 2026
>
> **Scope:** Core engine only. No Dagster integration, no platform adapters (Databricks/Snowflake/Spark),
> no remote execution. Just docbt running locally, processing documents, tracking state with Metaxy,
> running tests, generating docs.
>
> **Goal:** `docbt run` processes a directory of PDFs into structured output with incremental
> processing, testing, and documentation — all from YAML config.

---

## 1. What We're Building (and NOT Building)

### IN SCOPE

| Component | Description |
|-----------|-------------|
| Rust CLI | `docbt init`, `run`, `test`, `compile`, `docs`, `graph`, `clean` |
| YAML config system | Project, sources, models with serde parsing |
| DAG resolution | petgraph-based dependency graph with `--select` / `--exclude` |
| Extraction backends | Docling (primary), Marker (secondary), Custom Python |
| Metaxy integration | FeatureSpec generation, resolve_update(), DuckDB store |
| Local execution | Process documents on your machine, parallel via rayon |
| Testing framework | Schema tests + custom Python tests |
| Docs generation | Static HTML site with Mermaid DAG, model pages |
| Local output | JSON, Markdown, Parquet files to local filesystem |

### OUT OF SCOPE (for now)

| Component | Why Later |
|-----------|-----------|
| Dagster integration | Integration layer, not core |
| Platform adapters (Databricks, Snowflake, Spark) | Requires remote compute infrastructure |
| Remote metadata stores (ClickHouse, BigQuery) | DuckDB is sufficient for prototype |
| S3/GCS/ADLS source loading | Local filesystem first |
| LLM-based entity extraction transforms | Backend extraction first, transforms later |
| MCP server | Nice-to-have, not core |

---

## 2. Architecture (Simplified)

```
┌────────────────────────────────────────────────────┐
│                  docbt CLI (Rust)                    │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │  Config   │  │   DAG    │  │   CLI Commands   │  │
│  │  Parser   │  │ Resolver │  │                  │  │
│  │  (serde)  │  │(petgraph)│  │  init, run,      │  │
│  │           │  │          │  │  test, compile,  │  │
│  │           │  │          │  │  docs, graph     │  │
│  └─────┬─────┘  └────┬─────┘  └───────┬──────────┘  │
│        └──────────────┼────────────────┘             │
│                       │                              │
│  ┌────────────────────▼───────────────────────────┐  │
│  │            Execution Engine                     │  │
│  │  • File discovery (glob)                        │  │
│  │  • Parallel dispatch (rayon)                    │  │
│  │  • Progress bars (indicatif)                    │  │
│  │  • Run result collection                        │  │
│  └────────────────────┬───────────────────────────┘  │
│                       │                              │
│  ┌────────────────────▼───────────────────────────┐  │
│  │          Python Bridge (PyO3)                   │  │
│  │                                                 │  │
│  │  ┌───────────────┐  ┌───────────────────────┐  │  │
│  │  │  Backends     │  │  Metaxy Layer         │  │  │
│  │  │               │  │                       │  │  │
│  │  │  • Docling    │  │  • YAML → FeatureSpec │  │  │
│  │  │  • Marker     │  │  • DuckDB store       │  │  │
│  │  │  • Custom     │  │  • resolve_update()   │  │  │
│  │  └───────────────┘  └───────────────────────┘  │  │
│  │                                                 │  │
│  │  ┌───────────────┐  ┌───────────────────────┐  │  │
│  │  │  Test Runner  │  │  Output Writer        │  │  │
│  │  │               │  │                       │  │  │
│  │  │  • Schema     │  │  • JSON               │  │  │
│  │  │  • Custom     │  │  • Markdown            │  │  │
│  │  │               │  │  • Parquet             │  │  │
│  │  └───────────────┘  └───────────────────────┘  │  │
│  └─────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

---

## 3. Project Structure

```
docbt/
├── Cargo.toml                        # Workspace root
├── pyproject.toml                    # Python package (maturin build)
├── CLAUDE.md                         # Claude Code instructions
├── README.md
│
├── crates/
│   ├── docbt-cli/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs               # Entry point
│   │       └── commands/
│   │           ├── mod.rs
│   │           ├── init.rs            # docbt init <project_name>
│   │           ├── compile.rs         # docbt compile (validate config + show DAG)
│   │           ├── run.rs             # docbt run [--select] [--full-refresh]
│   │           ├── test.rs            # docbt test [--select]
│   │           ├── docs.rs            # docbt docs generate | serve
│   │           ├── graph.rs           # docbt graph (render DAG)
│   │           ├── clean.rs           # docbt clean
│   │           └── backend.rs         # docbt backend list | validate
│   │
│   ├── docbt-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── error.rs              # DocbtError with thiserror
│   │       ├── config/
│   │       │   ├── mod.rs
│   │       │   ├── project.rs        # docbt_project.yml
│   │       │   ├── source.rs         # sources/*.yml
│   │       │   ├── model.rs          # models/*.yml
│   │       │   └── profile.rs        # Profile resolution
│   │       ├── dag/
│   │       │   ├── mod.rs
│   │       │   ├── graph.rs          # petgraph DAG construction
│   │       │   └── selector.rs       # --select / --exclude parsing
│   │       └── runner/
│   │           ├── mod.rs
│   │           ├── discovery.rs      # Source file discovery (glob + hashing)
│   │           ├── executor.rs       # Parallel execution orchestration
│   │           └── result.rs         # RunResult, ModelResult types
│   │
│   └── docbt-python/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                # PyO3 module definition
│           ├── bridge.rs             # Main dispatch: Rust calls Python
│           └── types.rs              # Shared types (Rust ↔ Python)
│
├── python/
│   └── docbt/
│       ├── __init__.py
│       ├── backends/
│       │   ├── __init__.py
│       │   ├── base.py              # BaseBackend ABC
│       │   ├── registry.py          # Backend discovery + validation
│       │   ├── docling_backend.py   # Docling extraction
│       │   ├── marker_backend.py    # Marker extraction
│       │   └── custom_backend.py    # Load user Python modules
│       ├── metaxy_layer/
│       │   ├── __init__.py
│       │   ├── feature_gen.py       # YAML model → Metaxy FeatureSpec
│       │   ├── store.py             # MetadataStore setup (DuckDB)
│       │   └── versioning.py        # Code version string generation
│       ├── output/
│       │   ├── __init__.py
│       │   └── writer.py            # Write JSON / Markdown / Parquet locally
│       ├── testing/
│       │   ├── __init__.py
│       │   ├── schema_tests.py      # Built-in schema test implementations
│       │   └── runner.py            # Test discovery + execution
│       └── project.py               # DocbtProject: ties everything together
│
├── templates/                        # docbt init scaffolding
│   ├── docbt_project.yml
│   ├── sources/
│   │   └── example.yml
│   ├── models/
│   │   └── example.yml
│   └── tests/
│       └── .gitkeep
│
├── docs_site/                        # docbt docs generate template
│   ├── index.html
│   └── templates/
│       ├── model.html
│       ├── source.html
│       └── lineage.html
│
└── examples/
    └── invoice_pipeline/
        ├── docbt_project.yml
        ├── sources/
        │   └── invoices.yml
        ├── models/
        │   ├── raw_invoices.yml
        │   └── invoice_summary.yml
        ├── tests/
        │   └── test_invoice_fields.py
        └── sample_data/
            ├── invoice_001.pdf
            ├── invoice_002.pdf
            └── invoice_003.pdf
```

---

## 4. Config Schema (Final)

### 4.1 `docbt_project.yml`

```yaml
name: invoice_processing
version: "0.1.0"
config-version: 2

extraction:
  default_backend: docling

metaxy:
  store:
    type: duckdb
    path: ./target/metaxy.duckdb
  id_column: document_id

output:
  default_format: json          # json | markdown | parquet
  path: ./output

source-paths: ["sources"]
model-paths: ["models"]
test-paths: ["tests"]
target-path: "target"

clean-targets:
  - "target"
  - "output"
```

### 4.2 `sources/invoices.yml`

```yaml
version: 2

sources:
  - name: vendor_invoices
    description: "Monthly vendor invoices from accounting"
    path: "./data/invoices/"
    file_pattern: "*.pdf"
    recursive: true
    freshness:
      warn_after: { count: 24, period: hour }
      error_after: { count: 48, period: hour }
    meta:
      owner: finance_team
```

### 4.3 `models/raw_invoices.yml`

```yaml
version: 2

models:
  - name: raw_invoices
    description: "Extract raw text and tables from vendor invoices"
    source: ref('vendor_invoices')

    extraction:
      backend: docling
      options:
        extract_tables: true
        ocr_fallback: true

    output:
      format: json
      one_file_per: document

    materialization: incremental

    fields:
      - name: text
        description: "Full extracted document text"
      - name: tables
        description: "Extracted table data"
      - name: metadata
        description: "Extraction metadata and stats"

    tests:
      - has_text
      - not_empty
      - min_pages: 1

    meta:
      owner: data_engineering
      tags: ["invoices", "raw"]
```

### 4.4 `models/invoice_summary.yml`

```yaml
version: 2

models:
  - name: invoice_summary
    description: "Summarized invoice data per document"
    depends_on:
      - ref('raw_invoices')

    fields:
      - name: page_count
        description: "Number of pages"
        version_from: [ref('raw_invoices').metadata]
      - name: table_count
        description: "Number of tables found"
        version_from: [ref('raw_invoices').tables]
      - name: word_count
        description: "Total word count"
        version_from: [ref('raw_invoices').text]

    transform:
      type: python
      module: transforms.summarize

    output:
      format: parquet

    materialization: incremental

    tests:
      - not_null: [page_count, word_count]
```

---

## 5. Rust Crate Details

### 5.1 Dependencies

```toml
# Cargo.toml (workspace)
[workspace]
members = ["crates/docbt-cli", "crates/docbt-core", "crates/docbt-python"]
resolver = "2"

# crates/docbt-core/Cargo.toml
[dependencies]
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
serde_json = "1"
petgraph = "0.7"
blake3 = "1"
glob = "0.3"
chrono = { version = "0.4", features = ["serde"] }
thiserror = "2"
tracing = "0.1"

# crates/docbt-cli/Cargo.toml
[dependencies]
docbt-core = { path = "../docbt-core" }
docbt-python = { path = "../docbt-python" }
clap = { version = "4", features = ["derive"] }
indicatif = "0.17"
colored = "3"
comfy-table = "7"
tracing-subscriber = "0.3"

# crates/docbt-python/Cargo.toml
[dependencies]
docbt-core = { path = "../docbt-core" }
pyo3 = { version = "0.23", features = ["auto-initialize"] }
```

### 5.2 Core Types

```rust
// crates/docbt-core/src/config/project.rs
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct ProjectConfig {
    pub name: String,
    pub version: String,
    #[serde(rename = "config-version")]
    pub config_version: u32,

    #[serde(default)]
    pub extraction: ExtractionDefaults,

    #[serde(default)]
    pub metaxy: MetaxyConfig,

    #[serde(default)]
    pub output: OutputDefaults,

    #[serde(rename = "source-paths", default = "default_source_paths")]
    pub source_paths: Vec<PathBuf>,

    #[serde(rename = "model-paths", default = "default_model_paths")]
    pub model_paths: Vec<PathBuf>,

    #[serde(rename = "test-paths", default = "default_test_paths")]
    pub test_paths: Vec<PathBuf>,

    #[serde(rename = "target-path", default = "default_target_path")]
    pub target_path: PathBuf,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ExtractionDefaults {
    #[serde(default = "default_backend")]
    pub default_backend: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MetaxyConfig {
    pub store: MetaxyStoreConfig,
    #[serde(default = "default_id_column")]
    pub id_column: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum MetaxyStoreConfig {
    #[serde(rename = "duckdb")]
    DuckDB { path: PathBuf },
}

fn default_backend() -> String { "docling".into() }
fn default_id_column() -> String { "document_id".into() }
fn default_source_paths() -> Vec<PathBuf> { vec!["sources".into()] }
fn default_model_paths() -> Vec<PathBuf> { vec!["models".into()] }
fn default_test_paths() -> Vec<PathBuf> { vec!["tests".into()] }
fn default_target_path() -> PathBuf { "target".into() }
```

```rust
// crates/docbt-core/src/config/source.rs
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
pub struct SourceFile {
    pub version: u32,
    pub sources: Vec<SourceConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SourceConfig {
    pub name: String,
    pub description: Option<String>,
    pub path: String,
    #[serde(default = "default_file_pattern")]
    pub file_pattern: String,
    #[serde(default)]
    pub recursive: bool,
    pub freshness: Option<FreshnessConfig>,
    #[serde(default)]
    pub meta: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FreshnessConfig {
    pub warn_after: Option<DurationSpec>,
    pub error_after: Option<DurationSpec>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DurationSpec {
    pub count: u64,
    pub period: String, // hour, day, week
}

fn default_file_pattern() -> String { "*.pdf".into() }
```

```rust
// crates/docbt-core/src/config/model.rs
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
pub struct ModelFile {
    pub version: u32,
    pub models: Vec<ModelConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ModelConfig {
    pub name: String,
    pub description: Option<String>,
    pub source: Option<String>,           // ref('source_name')
    pub depends_on: Option<Vec<String>>,  // [ref('other_model')]

    pub extraction: Option<ExtractionConfig>,
    pub transform: Option<TransformConfig>,

    #[serde(default)]
    pub fields: Vec<FieldConfig>,

    pub output: Option<OutputConfig>,

    #[serde(default = "default_materialization")]
    pub materialization: String,

    #[serde(default)]
    pub tests: Vec<serde_yaml::Value>,    // Flexible: string or map

    #[serde(default)]
    pub meta: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExtractionConfig {
    pub backend: Option<String>,
    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TransformConfig {
    #[serde(rename = "type")]
    pub transform_type: String,
    pub module: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FieldConfig {
    pub name: String,
    pub description: Option<String>,
    pub version_from: Option<Vec<String>>,  // field-level deps
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OutputConfig {
    #[serde(default = "default_format")]
    pub format: String,
    pub one_file_per: Option<String>,
}

fn default_materialization() -> String { "full".into() }
fn default_format() -> String { "json".into() }
```

```rust
// crates/docbt-core/src/dag/graph.rs
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::algo::toposort;
use std::collections::HashMap;

use crate::config::model::ModelConfig;
use crate::config::source::SourceConfig;
use crate::error::DocbtError;

#[derive(Debug, Clone)]
pub enum NodeKind {
    Source(String),
    Model(String),
}

pub struct ProjectGraph {
    graph: DiGraph<NodeKind, ()>,
    name_to_index: HashMap<String, NodeIndex>,
}

impl ProjectGraph {
    pub fn build(
        sources: &[SourceConfig],
        models: &[ModelConfig],
    ) -> Result<Self, DocbtError> {
        let mut graph = DiGraph::new();
        let mut name_to_index = HashMap::new();

        // Add source nodes
        for source in sources {
            let idx = graph.add_node(NodeKind::Source(source.name.clone()));
            name_to_index.insert(source.name.clone(), idx);
        }

        // Add model nodes
        for model in models {
            let idx = graph.add_node(NodeKind::Model(model.name.clone()));
            name_to_index.insert(model.name.clone(), idx);
        }

        // Add edges from ref() declarations
        for model in models {
            let model_idx = name_to_index[&model.name];

            if let Some(ref source_ref) = model.source {
                let source_name = parse_ref(source_ref)?;
                if let Some(&source_idx) = name_to_index.get(&source_name) {
                    graph.add_edge(source_idx, model_idx, ());
                } else {
                    return Err(DocbtError::UnknownRef {
                        model: model.name.clone(),
                        reference: source_name,
                    });
                }
            }

            if let Some(ref deps) = model.depends_on {
                for dep_ref in deps {
                    let dep_name = parse_ref(dep_ref)?;
                    if let Some(&dep_idx) = name_to_index.get(&dep_name) {
                        graph.add_edge(dep_idx, model_idx, ());
                    } else {
                        return Err(DocbtError::UnknownRef {
                            model: model.name.clone(),
                            reference: dep_name,
                        });
                    }
                }
            }
        }

        // Check for cycles
        toposort(&graph, None).map_err(|_| DocbtError::CyclicDependency)?;

        Ok(Self { graph, name_to_index })
    }

    /// Return models in execution order
    pub fn execution_order(&self) -> Vec<&str> {
        toposort(&self.graph, None)
            .unwrap()
            .iter()
            .filter_map(|idx| match &self.graph[*idx] {
                NodeKind::Model(name) => Some(name.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Select a model and optionally its upstream (+) or downstream (+) deps
    pub fn select(&self, selector: &str) -> Result<Vec<&str>, DocbtError> {
        // Parse selectors like "model_name", "+model_name", "model_name+", "+model_name+"
        todo!("implement selector parsing")
    }

    /// Render as Mermaid diagram
    pub fn to_mermaid(&self) -> String {
        let mut out = String::from("graph LR\n");
        for edge in self.graph.edge_indices() {
            let (a, b) = self.graph.edge_endpoints(edge).unwrap();
            let a_name = match &self.graph[a] {
                NodeKind::Source(n) | NodeKind::Model(n) => n,
            };
            let b_name = match &self.graph[b] {
                NodeKind::Source(n) | NodeKind::Model(n) => n,
            };
            out.push_str(&format!("    {} --> {}\n", a_name, b_name));
        }
        out
    }
}

/// Parse "ref('name')" → "name"
fn parse_ref(s: &str) -> Result<String, DocbtError> {
    let s = s.trim();
    if s.starts_with("ref('") && s.ends_with("')") {
        Ok(s[5..s.len()-2].to_string())
    } else {
        // Bare name is also OK
        Ok(s.to_string())
    }
}
```

```rust
// crates/docbt-core/src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DocbtError {
    #[error("Config error: {message}")]
    Config { message: String },

    #[error("Model '{model}' references unknown '{reference}'")]
    UnknownRef { model: String, reference: String },

    #[error("Cyclic dependency detected in model graph")]
    CyclicDependency,

    #[error("Backend '{backend}' not installed or not available")]
    BackendNotAvailable { backend: String },

    #[error("Extraction failed for {path}: {reason}")]
    ExtractionFailed { path: String, reason: String },

    #[error("Test failed: {name} — {message}")]
    TestFailed { name: String, message: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("Python error: {message}")]
    Python { message: String },
}
```

### 5.3 CLI

```rust
// crates/docbt-cli/src/main.rs
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "docbt", version, about = "dbt for unstructured data")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new docbt project
    Init {
        /// Project name
        name: String,
    },
    /// Compile and validate config (no execution)
    Compile,
    /// Run extraction models
    Run {
        /// Select specific models (e.g. "model_name", "+model_name+")
        #[arg(long)]
        select: Option<String>,
        /// Exclude specific models
        #[arg(long)]
        exclude: Option<String>,
        /// Ignore incremental state, reprocess everything
        #[arg(long)]
        full_refresh: bool,
    },
    /// Run tests
    Test {
        /// Select specific models to test
        #[arg(long)]
        select: Option<String>,
    },
    /// Generate or serve documentation
    Docs {
        #[command(subcommand)]
        action: DocsAction,
    },
    /// Render the model dependency graph
    Graph {
        /// Output format
        #[arg(long, default_value = "mermaid")]
        format: String,
    },
    /// List and validate backends
    Backend {
        #[command(subcommand)]
        action: BackendAction,
    },
    /// Clean target directory
    Clean,
}

#[derive(Subcommand)]
enum DocsAction {
    Generate,
    Serve {
        #[arg(long, default_value = "8080")]
        port: u16,
    },
}

#[derive(Subcommand)]
enum BackendAction {
    List,
    Validate { name: String },
}

fn main() {
    let cli = Cli::parse();
    tracing_subscriber::fmt::init();

    let result = match cli.command {
        Commands::Init { name } => commands::init::run(&name),
        Commands::Compile => commands::compile::run(),
        Commands::Run { select, exclude, full_refresh } => {
            commands::run::run(select.as_deref(), exclude.as_deref(), full_refresh)
        }
        Commands::Test { select } => commands::test::run(select.as_deref()),
        Commands::Docs { action } => match action {
            DocsAction::Generate => commands::docs::generate(),
            DocsAction::Serve { port } => commands::docs::serve(port),
        },
        Commands::Graph { format } => commands::graph::run(&format),
        Commands::Backend { action } => match action {
            BackendAction::List => commands::backend::list(),
            BackendAction::Validate { name } => commands::backend::validate(&name),
        },
        Commands::Clean => commands::clean::run(),
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
```

---

## 6. Python Layer Details

### 6.1 Backend Interface

```python
# python/docbt/backends/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ExtractionResult:
    """Output of a single document extraction."""
    document_id: str
    source_path: str
    text: str = ""
    tables: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    pages: int = 0
    warnings: list[str] = field(default_factory=list)


class BaseBackend(ABC):
    """All extraction backends implement this."""

    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def supported_formats(self) -> list[str]: ...

    @abstractmethod
    def extract(self, path: Path, options: dict[str, Any]) -> ExtractionResult: ...

    @abstractmethod
    def validate(self) -> None:
        """Raise ImportError if dependencies missing."""
        ...
```

### 6.2 Metaxy Feature Generation

```python
# python/docbt/metaxy_layer/feature_gen.py
"""
Translates docbt YAML model configs into Metaxy FeatureSpecs.
This is the core integration point.
"""
from __future__ import annotations

from typing import Any

import metaxy as mx

from docbt.metaxy_layer.versioning import compute_code_version


def generate_feature_specs(
    models: list[dict[str, Any]],
    id_column: str = "document_id",
) -> dict[str, mx.FeatureSpec]:
    """
    Convert a list of parsed model configs into Metaxy FeatureSpecs.

    Returns a dict mapping model name → FeatureSpec.
    """
    specs: dict[str, mx.FeatureSpec] = {}
    feature_classes: dict[str, type[mx.BaseFeature]] = {}

    for model in models:
        name = model["name"]
        feature_key = f"docbt/{name}"

        # Build field specs
        field_specs = _build_field_specs(model, feature_classes)

        # Build dependency list
        deps = _resolve_deps(model, feature_classes)

        spec = mx.FeatureSpec(
            key=feature_key,
            id_columns=[id_column],
            fields=field_specs if field_specs else None,
            deps=deps if deps else None,
        )

        # Dynamically create the BaseFeature subclass
        feature_cls = type(
            _class_name(name),
            (mx.BaseFeature,),
            {"__annotations__": {id_column: str}},
            spec=spec,
        )

        specs[name] = spec
        feature_classes[name] = feature_cls

    return specs


def _build_field_specs(
    model: dict[str, Any],
    feature_classes: dict[str, type[mx.BaseFeature]],
) -> list[mx.FieldSpec]:
    """Build Metaxy FieldSpecs from model field definitions."""
    fields = model.get("fields", [])
    extraction = model.get("extraction", {})

    # Compute code version from extraction config
    code_version = compute_code_version(extraction)

    field_specs = []
    for f in fields:
        field_deps = None
        version_from = f.get("version_from")

        if version_from:
            field_deps = _parse_field_deps(version_from, feature_classes)

        field_specs.append(
            mx.FieldSpec(
                key=f["name"],
                code_version=code_version,
                deps=field_deps,
            )
        )

    # If no fields defined but extraction exists, create default fields
    if not field_specs and extraction:
        for default_field in ["text", "tables", "metadata"]:
            field_specs.append(
                mx.FieldSpec(key=default_field, code_version=code_version)
            )

    return field_specs


def _resolve_deps(
    model: dict[str, Any],
    feature_classes: dict[str, type[mx.BaseFeature]],
) -> list[type[mx.BaseFeature]]:
    """Resolve model dependencies to Metaxy feature classes."""
    deps = []

    # From source ref
    source = model.get("source", "")
    if source:
        ref_name = _parse_ref(source)
        if ref_name in feature_classes:
            deps.append(feature_classes[ref_name])

    # From depends_on
    for dep_ref in model.get("depends_on", []):
        ref_name = _parse_ref(dep_ref)
        if ref_name in feature_classes:
            deps.append(feature_classes[ref_name])

    return deps


def _parse_field_deps(
    version_from: list[str],
    feature_classes: dict[str, type[mx.BaseFeature]],
) -> list[mx.FieldDep]:
    """Parse 'ref('model').field' syntax into Metaxy FieldDeps."""
    field_deps = []
    for dep_str in version_from:
        if "." in dep_str:
            # ref('raw_invoices').text → feature=RawInvoices, fields=["text"]
            ref_part, field_name = dep_str.rsplit(".", 1)
            ref_name = _parse_ref(ref_part)
            if ref_name in feature_classes:
                field_deps.append(
                    mx.FieldDep(
                        feature=feature_classes[ref_name],
                        fields=[field_name],
                    )
                )
    return field_deps


def _parse_ref(s: str) -> str:
    """Parse ref('name') → name, or return bare string."""
    s = s.strip()
    if s.startswith("ref('") and s.endswith("')"):
        return s[5:-2]
    return s


def _class_name(model_name: str) -> str:
    """Convert snake_case model name to PascalCase class name."""
    return "".join(word.capitalize() for word in model_name.split("_"))
```

### 6.3 Code Version Generation

```python
# python/docbt/metaxy_layer/versioning.py
"""
Generate deterministic code version strings from extraction configs.
When the config changes, the code version changes, which triggers
Metaxy to mark all samples as stale.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def compute_code_version(extraction_config: dict[str, Any]) -> str:
    """
    Generate a deterministic version string from extraction config.

    Changes to backend, options, or backend version will produce
    a different code_version, triggering reprocessing via Metaxy.
    """
    if not extraction_config:
        return "none:1"

    backend = extraction_config.get("backend", "docling")
    options = extraction_config.get("options", {})

    # Canonical JSON serialization for determinism
    canonical = json.dumps(
        {"backend": backend, "options": options},
        sort_keys=True,
        separators=(",", ":"),
    )

    version_hash = hashlib.sha256(canonical.encode()).hexdigest()[:12]
    return f"{backend}:{version_hash}"
```

### 6.4 Project Orchestration

```python
# python/docbt/project.py
"""
DocbtProject: loads config, sets up Metaxy, runs models.
Called from Rust via PyO3.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import metaxy as mx

from docbt.backends.registry import get_backend
from docbt.metaxy_layer.feature_gen import generate_feature_specs
from docbt.metaxy_layer.store import create_store
from docbt.output.writer import write_results


@dataclass
class ModelRunResult:
    model_name: str
    documents_processed: int
    documents_skipped: int
    duration_seconds: float
    backend_used: str
    errors: list[str]


class DocbtProject:
    """Main orchestration class called from Rust."""

    def __init__(
        self,
        project_config: dict[str, Any],
        sources: list[dict[str, Any]],
        models: list[dict[str, Any]],
    ) -> None:
        self.config = project_config
        self.sources = sources
        self.models = models

        # Set up Metaxy
        metaxy_config = project_config.get("metaxy", {})
        self.store = create_store(metaxy_config.get("store", {}))
        self.id_column = metaxy_config.get("id_column", "document_id")

        # Generate feature specs from YAML
        self.feature_specs = generate_feature_specs(
            models, id_column=self.id_column
        )

    def run_model(
        self,
        model_name: str,
        source_files: list[dict[str, str]],
        full_refresh: bool = False,
    ) -> ModelRunResult:
        """
        Run a single model. Called by Rust executor for each
        model in topological order.
        """
        import time

        start = time.monotonic()
        model = next(m for m in self.models if m["name"] == model_name)
        extraction = model.get("extraction", {})
        backend_name = extraction.get(
            "backend",
            self.config.get("extraction", {}).get("default_backend", "docling"),
        )

        backend = get_backend(backend_name)
        options = extraction.get("options", {})
        output_config = model.get("output", {})

        # Resolve increment with Metaxy
        feature_key = f"docbt/{model_name}"
        processed = 0
        skipped = 0
        errors: list[str] = []

        with self.store:
            if not full_refresh and model.get("materialization") == "incremental":
                # Build samples DataFrame for Metaxy
                import polars as pl

                samples_df = pl.DataFrame({
                    self.id_column: [f["document_id"] for f in source_files],
                    "metaxy_provenance_by_field": [
                        {field["name"]: f["hash"] for field in model.get("fields", [{"name": "text"}, {"name": "tables"}, {"name": "metadata"}])}
                        for f in source_files
                    ],
                })

                increment = self.store.resolve_update(
                    feature_key, samples=samples_df
                )

                files_to_process = [
                    f for f in source_files
                    if f["document_id"] in (increment.new | increment.stale)
                ]
                skipped = len(source_files) - len(files_to_process)
            else:
                files_to_process = source_files

            # Extract each document
            results = []
            for file_info in files_to_process:
                try:
                    result = backend.extract(
                        Path(file_info["path"]), options
                    )
                    result.document_id = file_info["document_id"]
                    result.source_path = file_info["path"]
                    results.append(result)
                    processed += 1
                except Exception as e:
                    errors.append(f"{file_info['path']}: {e}")

            # Write output files
            output_path = Path(
                self.config.get("output", {}).get("path", "./output")
            )
            write_results(model_name, results, output_config, output_path)

            # Update Metaxy store with results
            # (write metadata for processed documents)

        duration = time.monotonic() - start
        return ModelRunResult(
            model_name=model_name,
            documents_processed=processed,
            documents_skipped=skipped,
            duration_seconds=round(duration, 2),
            backend_used=backend_name,
            errors=errors,
        )
```

---

## 7. Build Phases

### Phase 1: Rust Skeleton (Days 1-2)

```
├── Cargo workspace with 3 crates
├── Config structs (project, source, model) with serde
├── YAML parser that loads docbt_project.yml + source/model files
├── DAG construction with petgraph
├── Topological sort
├── CLI skeleton with clap (all subcommands stubbed)
├── docbt init (scaffold template project)
├── docbt compile (parse + validate + print DAG summary)
├── docbt graph (output Mermaid diagram)
├── Error types with thiserror
└── Unit tests for config parsing + DAG
```

**Exit criteria:** `docbt compile` parses YAML and prints model execution order. `docbt graph` outputs Mermaid.

### Phase 2: Python Layer + Metaxy (Days 3-5)

```
├── Python project setup (pyproject.toml with metaxy + docling deps)
├── BaseBackend interface
├── DoclingBackend implementation
├── Backend registry + validation
├── feature_gen.py (YAML → FeatureSpec)
├── versioning.py (config → code version)
├── store.py (DuckDB MetadataStore setup)
├── output/writer.py (JSON + Markdown output)
├── project.py (DocbtProject orchestration)
└── Integration test: parse YAML → generate features → extract 1 PDF
```

**Exit criteria:** Python layer can take a model config, extract a PDF with Docling, and write JSON output. Metaxy tracks the extraction.

### Phase 3: Rust ↔ Python Bridge (Days 6-8)

```
├── PyO3 bridge: Rust calls DocbtProject.run_model()
├── Source file discovery (glob patterns in Rust)
├── Document ID generation (blake3 hash of path + mtime)
├── Parallel model dispatch (rayon for file-level parallelism)
├── docbt run command (end-to-end)
├── Progress bars with indicatif
├── Run result reporting (table of model results)
├── Incremental: second run skips unchanged files
└── --full-refresh flag
```

**Exit criteria:** `docbt run` processes a directory of PDFs end-to-end. Second run is faster (incremental).

### Phase 4: Testing (Days 9-10)

```
├── Schema test implementations (has_text, not_empty, not_null, etc.)
├── Test discovery (parse test configs from model YAML)
├── Custom Python test loader
├── Test runner (execute after model run)
├── docbt test command
├── Test result reporting (pass/warn/fail table)
└── --select for running specific model tests
```

**Exit criteria:** `docbt test` validates extraction output against schema tests.

### Phase 5: Docs + Polish (Days 11-14)

```
├── Manifest generation (target/manifest.json)
├── Catalog generation (target/catalog.json)
├── Static HTML docs site generation
│   ├── Index page with project overview
│   ├── Model pages (description, config, tests, lineage)
│   ├── Source pages (path, freshness, meta)
│   └── Lineage page (Mermaid DAG)
├── docbt docs generate + docbt docs serve (simple HTTP server)
├── docbt backend list (table of installed backends)
├── docbt backend validate <name>
├── docbt clean
├── Example project with 3 sample PDFs
├── CLAUDE.md
├── README.md with quickstart
└── Error messages review (make them helpful)
```

**Exit criteria:** Full working prototype. `docbt docs serve` shows browsable docs with lineage.

---

## 8. CLAUDE.md

```markdown
# CLAUDE.md — docbt

## What is this
docbt is "dbt for unstructured data." Rust CLI + Python extraction layer.
Declarative YAML config, incremental processing via Metaxy, pluggable backends.

## Architecture
- Rust workspace: docbt-cli, docbt-core, docbt-python (PyO3)
- Python: backends (Docling, Marker), Metaxy integration, testing
- Config: YAML parsed with serde
- DAG: petgraph
- State: Metaxy with DuckDB MetadataStore

## Build & Run
cargo build                          # Build Rust
cargo run -p docbt-cli -- compile    # Compile config
cargo run -p docbt-cli -- run        # Run pipeline
uv sync                              # Install Python deps
uv run pytest                        # Python tests

## Key Files
- Config types: crates/docbt-core/src/config/
- DAG: crates/docbt-core/src/dag/graph.rs
- CLI: crates/docbt-cli/src/main.rs
- Python bridge: crates/docbt-python/src/bridge.rs
- Backend interface: python/docbt/backends/base.py
- Metaxy integration: python/docbt/metaxy_layer/feature_gen.py
- Project orchestration: python/docbt/project.py

## Conventions
- Rust: clippy clean, no unwrap() in lib code, thiserror for errors
- Python: type hints everywhere, pathlib not os.path, ruff for linting
- YAML: 2-space indent, snake_case keys
- Tests: Rust #[cfg(test)] inline, Python pytest in tests/

## Config Files
- docbt_project.yml — project root config
- sources/*.yml — document source definitions
- models/*.yml — extraction model definitions

## Metaxy
- Each model becomes a Metaxy FeatureSpec
- DuckDB MetadataStore at ./target/metaxy.duckdb
- resolve_update() determines what needs reprocessing
- Code versions computed from backend + options hash
```

---

## 9. First Session Checklist

```bash
# 1. Create workspace
mkdir docbt && cd docbt

# 2. Scaffold Rust
# - Cargo.toml (workspace)
# - crates/docbt-core/ (config, dag, error)
# - crates/docbt-cli/ (main.rs, commands/)
# - crates/docbt-python/ (stub)

# 3. Implement config parsing
# - ProjectConfig, SourceConfig, ModelConfig structs
# - Load and parse docbt_project.yml
# - Load sources/*.yml and models/*.yml

# 4. Implement DAG
# - ProjectGraph::build()
# - Topological sort
# - Mermaid output

# 5. Wire up CLI
# - docbt compile → parse + validate + print
# - docbt graph → Mermaid output
# - docbt init → scaffold template

# 6. Write the example project
# - examples/invoice_pipeline/ with sample YAMLs

# 7. Tests
# - Config parsing tests
# - DAG construction tests
# - ref() parsing tests

# TARGET: `docbt compile` and `docbt graph` work on the example project
```
