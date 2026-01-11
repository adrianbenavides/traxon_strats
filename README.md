# Traxon Strats

![CI](https://github.com/adrianbenavides/traxon_strats/actions/workflows/main.yml/badge.svg)
![OSV scan](https://github.com/adrianbenavides/traxon_strats/actions/workflows/osv-scan.yml/badge.svg)

> [!WARNING]
> This project is currently in **Beta**. It is under active development and should **not** be used in production trading
> systems.

## Project Overview

Implementation of crypto trading strategies and durable orchestration flows built on
the [Traxon Core](https://github.com/adrianbenavides/traxon_core) library.

This repository contains:

- **Strategy Logic:** Retail crypto trading strategies (e.g., RobotWealth's YOLO) with signal processing, order
  building, and execution.
- **Durable Orchestration:** Temporal-based workflows and activities for resilient strategy execution and trade
  management.
- **API Integrations:** Clients for external providers like Robot Wealth and various crypto exchanges.
- **Persistence:** Local repository implementations (DuckDB) for strategy parameters and account history.

## Getting Started

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

## Development

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

## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feat/my-new-strategy`).
3. Follow the **Conductor** spec-driven development workflow.
4. Commit your changes (`git commit -m "feat(strategy): add new alpha factor"`).
5. Open a Pull Request.
