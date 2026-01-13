# Traxon Strats

![CI](https://github.com/adrianbenavides/traxon_strats/actions/workflows/main.yml/badge.svg)
![OSV scan](https://github.com/adrianbenavides/traxon_strats/actions/workflows/osv-scan.yml/badge.svg)

> [!WARNING]
> This project is currently in **Beta**. It is under active development and should **not** be used in production trading
> systems.

## 🔭 Project Overview

Implementation of crypto trading strategies and durable orchestration flows built on
the [Traxon Core](https://github.com/adrianbenavides/traxon_core) library.

The primary goal of this library is to move away from fragile loops and cron jobs toward
**durable, type-safe execution**.

### ✨ Key Features

- **Strategy Logic:** Retail crypto trading strategies (currently running **RobotWealth's YOLO** as a proof-of-concept)
  with signal processing, order
  building, and execution.
- **Durable Orchestration:** Temporal-based workflows and activities for resilient strategy execution and trade
  management.
- **API Integrations:** Clients for external providers like Robot Wealth and various crypto exchanges.
- **Persistence:** Local repository implementations (DuckDB) for strategy parameters and account history.

#### ⚡ Why Temporal?

We use **Temporal** to define trading flows with automatic retries, state persistence across failures, and built-in
visibility into workflow execution history.

- **True Durability:** If the server dies or the API disconnects, the strategy doesn't "reset." It resumes exactly where
  it left off, automatically handling retries, timeouts, and long-running state.
- **Decoupled Infrastructure:** Unlike frameworks that often couple definition with deployment (e.g., Dagster), Temporal
  allows us to define workflows and activities purely as code. This means you can plug these strategy flows directly
  into your own existing Temporal applications or workers.

### 🛡️ Python & Type Safety

We leverage modern **Python 3.12+** features to build a robust and crash-resistant trading system. The codebase
prioritizes strict correctness through comprehensive type safety and validation:

- **Static Analysis:** We use **Mypy** in strict mode to enforce type correctness at compile time, catching errors
  before code is ever run.
- **Runtime Validation:** **Pydantic** models ensure that all data flowing through the system—from config files to API
  responses—conforms to strict schemas.
- **Runtime Checking:** Critical components are protected by **Beartype**, providing fast O(1) runtime type checking to
  catch type violations during execution.
- **Data Integrity:** **Pandera** is used to validate dataframe schemas and statistical properties, ensuring that
  strategy signals and market data meet expected quality standards.
- **Explicit Error Handling:** We follow a **fail-fast** philosophy where functions raise explicit exceptions instead of
  returning invalid states (like `None`, `0`, or empty strings). This ensures that errors are handled immediately and
  never propagate silently through the system.

## 🏁 Getting Started

### Prerequisites

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)**

### Installation

To set up the development environment:

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd traxon-strats
   ```

2. **Sync dependencies:**
   ```bash
   uv sync
   ```

## 🛠️ Development

### Running Tests

To run the unit test suite:

```bash
uv run poe test
```

### Static Analysis & Linting

We enforce strict type checking and consistent formatting:

```bash
# Run all checks (format + lint + types)
uv run poe lint

# Type checking only
uv run poe lint-types
```

### Code Formatting

Format your code before committing:

```bash
uv run poe format
```

## 🤝 Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feat/my-new-strategy`).
3. Follow the **Conductor** spec-driven development workflow.
4. Commit your changes (`git commit -m "feat(strategy): add new alpha factor"`).
5. Open a Pull Request.