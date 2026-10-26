# LinkedIn showcase material

These drafts describe the implementation and measurements verified in the local repository. Review and personalize them before publishing.

## A. LinkedIn project entry

**Million Song Dataset Data Engineering Pipeline**

Built a local-first data engineering workflow for ingesting HDF5 song metadata into Parquet, modeling it in DuckDB with dbt, and exploring results in a read-only Streamlit dashboard. The pipeline tracks source changes with a manifest and SHA-256 signatures, retains previously valid records when a changed file fails extraction, and validates analytical models with dbt tests. The current local dataset contains 10,000 song rows and 3,888 artist dimension rows. The project uses pytest, Ruff, and GitHub Actions for automated quality checks.

## B. LinkedIn launch post

I’ve been building a data engineering portfolio project around the Million Song Dataset subset, focusing on the parts of a pipeline that make repeated runs safer and analytics easier to trust.

The workflow extracts HDF5 records with Python, writes processed data to Parquet, and uses a manifest with file metadata and SHA-256 hashes to detect changes. DuckDB and dbt provide staging, artist dimension, song audio fact, and yearly summary models. I also built a read-only Streamlit dashboard for dataset metrics, artist rankings, audio features, release years, and searchable song records.

I added regression coverage for incremental ingestion, dashboard query helpers, and subprocess timeout/cancellation behavior, and run pytest, Ruff, and GitHub Actions. Locally, the current dataset has 10,000 song records and the dbt build passes 11 data tests.

One lesson from the work: tracking only file size and modification time can miss content changes when those values are preserved, so I added content hashing and a regression test. I also made the artist chart join the fact model to the artist dimension rather than expecting an artist name in the fact table.

I’m continuing to improve the project and would appreciate feedback on the model design, incremental-processing trade-offs, or what would make the repository clearer to another data engineer.

Repository: https://github.com/hussainali99a/Million-Song-Dataset-Analytics-Pipeline

## C. Resume bullets

- Built an incremental Python ingestion pipeline for 10,000 HDF5 song files, storing records in Parquet and using SHA-256 signatures to detect content changes.
- Modeled 10,000 song facts, 3,888 artist dimension rows, and 68 yearly aggregates with DuckDB and dbt; added key-integrity and data-quality tests.
- Added failure recovery that retains prior valid records for failed source replacements and records failed files for later retries.
- Implemented a read-only Streamlit dashboard with a deduplicated artist-dimension join, audio-feature histograms, quality metrics, and searchable records.
- Added automated tests and Ruff checks; local validation reported 52 passing tests, one dataset-dependent skip, and 74% combined coverage across `src`, dashboard query helpers, and pipeline orchestration.

## D. Demonstrated skills

- Python data engineering and HDF5 extraction
- Incremental ingestion, file signatures, and failure recovery
- Parquet and PyArrow
- DuckDB SQL and read-only analytics
- dbt staging, dimensional modeling, and data tests
- Streamlit and Plotly
- pytest, test fixtures, and coverage
- Ruff and GitHub Actions
- Windows-oriented local development and path portability

## E. Publication checklist

- [ ] Push the reviewed final code to GitHub.
- [ ] Verify the public repository link and repository visibility.
- [ ] Verify the latest GitHub Actions run is green for the pushed commit.
- [ ] Run the dashboard locally and capture a genuine screenshot.
- [ ] Confirm the README setup and launch instructions work from a clean environment.
- [ ] Check that no secrets, raw dataset files, or unintended generated artifacts are exposed.
- [ ] Resolve the repository's software license and dataset attribution/usage terms before publication.
- [ ] Add the GitHub repository link to LinkedIn.
- [ ] Personalize and review the project description and post before publishing.

These materials are drafts only. They have not been posted to LinkedIn and the repository has not been pushed by this task.
