# PROMPTS.md

**LLM used:** Claude (Anthropic), Sonnet 5 model, via the Claude Code CLI.

This document lists the prompts used to build this project, in the order
they were given. Each maps to a phase of the deliverable defined in the
challenge brief (IMDb Lakehouse-to-OLAP Pipeline).

---

### Prompt 1 — Build the pipeline

> Build a local data pipeline for the IMDb Lakehouse-to-OLAP challenge:
> download the IMDb dataset from Kaggle, extract title/rating/episode data,
> process it with PySpark, write it out as partitioned Snappy Parquet (the
> "lake"), and load it into an OLAP engine for high-speed analytics.
>
> Requirements: a `docker-compose.yml` running a Spark master/worker
> cluster plus the OLAP engine; a PySpark ETL job (`etl_job.py`) that
> cleans titles/ratings/episodes and writes partitioned Parquet using a
> partitioning strategy suited to time-series and category-based
> analysis; a loader (`load_to_olap.py`) that ingests the Parquet lake
> into the OLAP engine; DDL for the OLAP schema with indexes/primary
> keys; an analytics layer with queries answering a variety of questions
> about the data; a benchmark demonstrating the OLAP engine is
> significantly faster than querying Spark directly; a README with a
> performance note justifying the OLAP engine choice; and this
> `PROMPTS.md` file. The pipeline must actually run end-to-end, not just
> read as correct.

### Prompt 2 — Justify and pick the OLAP engine

> Compare OLAP engine options for this pipeline and pick one, considering
> that it needs to run locally in Docker alongside the Spark cluster and
> serve sub-second aggregate queries over tens of millions of rows.
> Document the reasoning in the README.

### Prompt 3 — Verify end-to-end on real data, not just review the code

> Don't stop at code that looks correct — actually stand up the Docker
> Compose stack, download the real IMDb dataset, run the ETL job against
> it, load the result into the OLAP engine, run every analytics query,
> and run the benchmark. Fix whatever breaks in practice.

### Prompt 4 — Confirm the Kaggle download requirement is genuinely met

> The brief specifically says download from Kaggle. Confirm the download
> script actually pulls from Kaggle itself, not just an equivalent mirror,
> and verify the dataset kaggle.com serves actually contains everything
> the pipeline needs — don't assume it matches the IMDb source files.

### Prompt 5 — Push to GitHub

> Publish this as a GitHub repository containing all the code, so it can
> be shared and reviewed as a complete deliverable.

### Prompt 6 — Production-quality pass

> Review the whole repo as if a senior engineer were about to review it
> for production: no hardcoded credentials, no dead or unreferenced
> files, consistent naming, and comments that earn their place — remove
> any that just restate the code instead of explaining a non-obvious
> decision. Re-verify end-to-end after cleanup, since a cleanup pass can
> silently break something.

### Prompt 7 — Documentation pass

> Rewrite `README.md` and `PROMPTS.md`. The README should be tight and
> self-explanatory — a reviewer should be able to understand the project
> and run it from the README alone, without wading through explanation
> that belongs in commit history.
