# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-faostat** scrapes food security data from [FAOSTAT](http://www.fao.org/faostat/en/) and creates one dataset per country in HDX. It makes a few reads from FAOSTAT and then ~1000 API calls to HDX per run. It runs every week.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.faostat
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_faostat.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline flows through stages in `__main__.py`:

1. **`get_countries`** — Reads the FAOSTAT country mapping file and returns a list of country dicts with ISO3 codes.

2. **`download_indicatorsets`** — Fetches the FAOSTAT dataset catalogue JSON and groups datasets by category.

3. **`generate_dataset_and_showcase`** — Constructs an HDX `Dataset` object and showcase for a given country and indicator category, downloads the CSV data, and attaches a resource.

### Key design points

- **One dataset per country per category**: the scraper iterates over countries and categories, creating/updating one HDX dataset each.
- **`Retrieve`** (`hdx-python-utilities`) abstracts HTTP downloads and supports save/replay via `save=True`/`use_saved=True` — used in tests to replay fixture data from `tests/fixtures/input/`.
- **Static config inside the package**: `config/` lives under `src/hdx/scraper/faostat/config/` so it is installed with the package and located via `script_dir_plus_file`.

### Config files

- `src/hdx/scraper/faostat/config/project_configuration.yaml` — API URL and dataset description template
- `src/hdx/scraper/faostat/config/hdx_dataset_static.yaml` — Static HDX metadata applied to every dataset (license, methodology, source, etc.)

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-faostat` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
