# MapReduce Word Count Dashboard

A friendly Streamlit dashboard that demonstrates how a threaded MapReduce pipeline turns uploaded text into useful word-frequency insights. Upload one or more text files, a folder, or a ZIP archive, run the pipeline, then explore and download the results.

## What the project does

The application processes uploaded text in memory through six stages:

1. **Input** — load supported `.txt` files and split the text into work chunks.
2. **Map** — count normalized Unicode words in parallel using Python's `ThreadPoolExecutor`.
3. **Combine** — merge mapper-local counts to reduce the data passed to reducers.
4. **Partition** — assign each word deterministically to one of two reducers.
5. **Reduce** — aggregate each reducer's assigned word counts concurrently.
6. **Output** — merge and sort the final counts for display and download.

The dashboard reports measurements from the current run, including word counts, stage timings, reducer distribution, and a chart of frequent words. Results can be searched, sorted, and downloaded in supported formats. It does not show example statistics as if they came from an upload.

## Features

- Upload individual or multiple `.txt` files, folders, and ZIP archives.
- Choose how input is split and set the number of mapper workers.
- Keep the reducer count fixed at exactly two.
- Inspect pipeline stages, metrics, searchable word counts, and charts.
- Download result files, including CSV and TXT output.
- Use a responsive, light pastel Streamlit interface.

## Run locally

Use Python 3.10 or later. From this directory, create and activate a virtual environment, install the dependencies, and start Streamlit:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
streamlit run app.py
```

## Project files

- `app.py` — Streamlit interface, upload handling, session state, and result presentation.
- `mapreduce.py` — the reusable MapReduce word-count implementation.
- `assets/styles.css` — dashboard styling.
- `assets/` — logo assets used by the application.
- `.streamlit/config.toml` — Streamlit theme configuration.

## Author

**Menna Allah Mohamed Adel**<br>
**ID:** 231014455

## Acknowledgments

With thanks to [Dr. Ahmed Said](https://github.com/dr-ahmed-said) for his guidance and support.
