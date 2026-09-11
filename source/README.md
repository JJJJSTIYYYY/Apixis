# APIXIS

An event-driven graph orchestration core for Python 3.12 and newer.

The `apixis.core` package provides event dispatch, graph execution, state
snapshots, streaming and interruption, without depending on an Agent SDK or
a database.

Run `uv sync --locked` and `uv run pytest` from this directory to install the
runtime and development dependencies and execute the tests.

Configuration is read from `./config.yaml` in the working directory. See the
repository's `docs/core/config/README.md` for the supported configuration and
migration notes.
