from __future__ import annotations

import csv
import hashlib
from html import escape
import io
import zipfile
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from mapreduce import REDUCER_COUNT, run_mapreduce


ASSET_DIR = Path(__file__).parent / "assets"
LOGO_MARK = ASSET_DIR / "logo-mark.png"
st.set_page_config(page_title="MapReduce Word Count", page_icon=str(LOGO_MARK), layout="wide")
for folder in ("input", "output"):
    (Path(__file__).parent / folder).mkdir(exist_ok=True)

PALETTE = {
    "ghost": "#F8F7FF", "antique": "#FFEEDD", "peach": "#FFD8BE",
    "periwinkle": "#B8B8FF", "primary": "#9381FF", "mint": "#BCEBDD",
    "coral": "#FF9C91", "ink": "#25264C", "muted": "#666987",
}
MAX_TOTAL_INPUT_BYTES = 200 * 1024 * 1024

styles = (Path(__file__).parent / "assets" / "styles.css").read_text(encoding="utf-8")
st.markdown(f"<style>{styles}</style>", unsafe_allow_html=True)

# Lucide-style inline SVG keeps the interface crisp without a remote font or CDN.
ICON_PATHS = {
    "file-up": '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7z"/><path d="M14 2v6h6"/><path d="M12 18v-6"/><path d="m9 15 3-3 3 3"/>',
    "layers": '<path d="m12 2 9 5-9 5-9-5 9-5Z"/><path d="m3 12 9 5 9-5"/><path d="m3 17 9 5 9-5"/>',
    "combine": '<path d="m12 2 8 4.5v9L12 20l-8-4.5v-9L12 2Z"/><path d="M12 20v-9"/><path d="m4.3 6.7 7.7 4.4 7.7-4.4"/>',
    "shuffle": '<path d="m18 14 4 4-4 4"/><path d="m18 2 4 4-4 4"/><path d="M2 18h2.5a6 6 0 0 0 4.2-1.7l6.6-6.6A6 6 0 0 1 19.5 8H22"/><path d="M2 6h2.5a6 6 0 0 1 4.2 1.7l1.1 1.1"/><path d="m14.2 14.2 1.1 1.1A6 6 0 0 0 19.5 17H22"/>',
    "cpu": '<rect x="5" y="5" width="14" height="14" rx="2"/><path d="M9 9h6v6H9z"/><path d="M9 1v4m6-4v4M9 19v4m6-4v4M1 9h4m-4 6h4m14-6h4m-4 6h4"/>',
    "chart": '<path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/>',
    "chart-pie": '<path d="M21.21 15.89A10 10 0 1 1 8.11 2.79"/><path d="M22 12A10 10 0 0 0 12 2v10z"/>',
    "sliders": '<path d="M4 21v-7m0-4V3m8 18v-9m0-4V3m8 18v-5m0-4V3"/><path d="M2 14h4m4-6h4m4 8h4"/>',
    "file-text": '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7z"/><path d="M14 2v6h6"/><path d="M8 13h8m-8 4h8"/>',
    "check": '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/>',
}


def icon_svg(name: str, size: int = 20) -> str:
    """Render a small, decorative Lucide-style inline icon."""
    return (f'<svg class="ui-icon" aria-hidden="true" focusable="false" '
            f'width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
            f'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" '
            f'stroke-linejoin="round">{ICON_PATHS[name]}</svg>')


def init_state() -> None:
    defaults = {"files": {}, "dismissed": set(), "upload_generation": 0,
                "result": None, "result_signature": None, "run_error": None,
                "failure_signature": None, "upload_mode": "Files",
                "mapper_workers": 4, "split_mode": "By lines"}
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def digest(name: str, data: bytes) -> str:
    return hashlib.sha256(name.encode("utf-8", "replace") + b"\0" + data).hexdigest()


def add_text_file(name: str, data: bytes, source: str = "upload") -> None:
    clean_name = name.replace("\\", "/").lstrip("/") or "untitled.txt"
    if not clean_name.lower().endswith(".txt"):
        st.session_state.setdefault("upload_messages", []).append(f"Skipped unsupported file: {clean_name}")
        return
    identity = digest(clean_name, data)
    if identity in st.session_state["dismissed"] or identity in st.session_state["files"]:
        return
    current_bytes = sum(len(item["data"]) for item in st.session_state["files"].values())
    if current_bytes + len(data) > MAX_TOTAL_INPUT_BYTES:
        st.session_state.setdefault("upload_messages", []).append(
            f"Skipped {clean_name}: the 200 MB total input limit would be exceeded."
        )
        return
    try:
        try:
            text = data.decode("utf-8-sig")
            encoding = "UTF-8"
        except UnicodeDecodeError:
            text = data.decode("cp1252")
            encoding = "Windows-1252 fallback"
    except (UnicodeDecodeError, LookupError):
        st.session_state.setdefault("upload_messages", []).append(f"Could not decode {clean_name}; file was skipped.")
        return
    st.session_state["files"][identity] = {"name": clean_name, "data": data, "text": text,
                                           "encoding": encoding, "source": source}


def process_zip(uploaded) -> None:
    data = uploaded.getvalue()
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = [info for info in archive.infolist() if not info.is_dir() and info.filename.lower().endswith(".txt")]
            total_uncompressed = sum(info.file_size for info in members)
            if total_uncompressed > 200 * 1024 * 1024:
                raise ValueError("The ZIP contains more than 200 MB of text files.")
            for info in members:
                if info.file_size > 50 * 1024 * 1024:
                    st.session_state.setdefault("upload_messages", []).append(f"Skipped oversized file: {info.filename}")
                    continue
                add_text_file(info.filename, archive.read(info), f"ZIP: {uploaded.name}")
            if not members:
                st.session_state.setdefault("upload_messages", []).append(f"No .txt files were found in {uploaded.name}.")
    except (zipfile.BadZipFile, OSError, RuntimeError, ValueError) as exc:
        st.session_state.setdefault("upload_messages", []).append(f"Could not read {uploaded.name}: {exc}")


def ingest_uploads() -> None:
    st.session_state.setdefault("upload_messages", [])
    for key in (f"files_{st.session_state.upload_generation}", f"folder_{st.session_state.upload_generation}"):
        for item in st.session_state.get(key, []) or []:
            try:
                add_text_file(item.name, item.getvalue())
            except (OSError, RuntimeError) as exc:
                st.session_state.upload_messages.append(f"Could not read {item.name}: {exc}")
    for item in st.session_state.get(f"zip_{st.session_state.upload_generation}", []) or []:
        process_zip(item)


def signature(files: dict, workers: int, split: str) -> str:
    h = hashlib.sha256()
    for key in sorted(files):
        h.update(key.encode())
    h.update(f"|{workers}|{split}".encode())
    return h.hexdigest()


def human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.2f} MB"


init_state()
ingest_uploads()
upload_messages = st.session_state.get("upload_messages", [])[-5:]
st.session_state["upload_messages"] = []

files = st.session_state.files
workers = int(st.session_state.mapper_workers)
split_mode = st.session_state.split_mode
header_logo, header_title, header_action = st.columns([0.6, 4, 1.35], gap="small", vertical_alignment="center")
header_logo.image(str(LOGO_MARK), width=72)
header_title.markdown('<div class="brand-title">MapReduce <span>Word Count</span></div><div class="brand-sub">Explore how threaded MapReduce turns text into useful insights.</div>', unsafe_allow_html=True)
run_clicked = header_action.button("Run MapReduce", type="primary", width="stretch",
                                   disabled=not files,
                                   help=None if files else "Upload a .txt file, folder, or ZIP to enable the run.",
                                   key="run_button")

if run_clicked:
    try:
        decoded = {
            item["name"] + (f" [{identity[:6]}]" if sum(1 for f in files.values() if f["name"] == item["name"]) > 1 else ""): item["text"]
            for identity, item in files.items()
        }
        with st.spinner("Running the real MapReduce pipeline…"):
            result = run_mapreduce(decoded, workers, split_mode=split_mode)
        st.session_state.result = result.to_dict()
        st.session_state.result_signature = signature(files, workers, split_mode)
        st.session_state.run_error = None
        st.session_state.failure_signature = None
        st.toast("MapReduce completed successfully.")
    except Exception as exc:
        st.session_state.run_error = str(exc)
        st.session_state.failure_signature = signature(files, workers, split_mode)
        st.session_state.result = None

current_signature = signature(files, workers, split_mode) if files else None
if st.session_state.run_error and current_signature != st.session_state.failure_signature:
    st.session_state.run_error = None
    st.session_state.failure_signature = None
if st.session_state.result is not None and current_signature != st.session_state.result_signature:
    st.session_state.result = None
    st.session_state.result_signature = None
if st.session_state.run_error:
    st.error(f"Processing failed: {st.session_state.run_error}")

left, center, right = st.columns([0.95, 1.95, 1.15], gap="small")
with left:
    st.markdown(
        f"<div class='panel-heading'>{icon_svg('file-up', 18)}<span>1 · Input & setup</span></div>",
        unsafe_allow_html=True,
    )
    with st.container(border=True):
        mode = st.selectbox("Upload source", ["Files", "Folder", "ZIP"], key="upload_mode",
                            help="Choose files, a directory, or a ZIP archive.")
        generation = st.session_state.upload_generation
        if mode == "Files":
            st.file_uploader("Add one or more .txt files", type=["txt"], accept_multiple_files=True,
                             max_upload_size=50, key=f"files_{generation}",
                             help="You can select several text files at once.")
        elif mode == "Folder":
            st.file_uploader("Select a folder of .txt files", type=["txt"], accept_multiple_files="directory",
                             max_upload_size=50, key=f"folder_{generation}",
                             help="Select a folder; text files in nested folders are included.")
        else:
            st.file_uploader("Add a ZIP containing .txt files", type=["zip"], accept_multiple_files=True,
                             max_upload_size=50, key=f"zip_{generation}",
                             help="ZIP works as a cross-browser folder-upload alternative.")

        for message in upload_messages:
            st.warning(message)

        total_bytes = sum(len(item["data"]) for item in files.values())
        if files:
            with st.expander(f"Selected files · {len(files)} · {human_size(total_bytes)}"):
                for identity, item in list(files.items()):
                    title = item["name"]
                    if sum(1 for f in files.values() if f["name"] == item["name"]) > 1:
                        title += f" · {identity[:6]}"
                    row_a, row_b = st.columns([5, 1], vertical_alignment="center")
                    row_a.markdown(
                        f"<div class='file-row-text'>{icon_svg('file-text', 16)}"
                        f"<span>{escape(title)} · {human_size(len(item['data']))} · {escape(item['encoding'])}</span></div>",
                        unsafe_allow_html=True,
                    )
                    if row_b.button("×", key=f"remove_{identity}", help=f"Remove {item['name']}"):
                        st.session_state.files.pop(identity, None)
                        st.session_state.dismissed.add(identity)
                        st.session_state.result = None
                        st.rerun()
                if st.button("Clear all files", key="clear_files", width="stretch"):
                    st.session_state.files.clear()
                    st.session_state.dismissed.clear()
                    st.session_state.upload_generation += 1
                    st.session_state.result = None
                    st.session_state.result_signature = None
                    st.rerun()
        else:
            st.caption("Choose .txt files, a folder, or a ZIP above to get started.")

    with st.container(border=True):
        st.markdown(
            f"<div class='settings-heading'>{icon_svg('sliders', 17)}<span>Processing settings</span></div>",
            unsafe_allow_html=True,
        )
        st.number_input("Mapper workers", min_value=1, max_value=32, step=1, key="mapper_workers")
        settings_col, reducer_col = st.columns([1.3, 1])
        settings_col.selectbox("Split mode", ["By lines", "By file"], key="split_mode",
                               help="By lines makes tasks of up to 500 lines; By file creates one task per file.")
        reducer_col.metric("Reducers", REDUCER_COUNT, help="Fixed at exactly two reducers.")
        st.caption("Normalization: lowercase Unicode words · numbers included · punctuation separates words")

with center:
    with st.container(border=True):
        title_col, status_col = st.columns([3, 1], vertical_alignment="center")
        title_col.markdown(
            f"<div class='section-title'>{icon_svg('layers', 21)}<span>2. MapReduce Pipeline</span></div>",
            unsafe_allow_html=True,
        )
        res = st.session_state.result
        pipeline_status = "Completed" if res else ("Failed" if st.session_state.run_error else "Not started")
        status_col.markdown(f'<span class="status-pill">{pipeline_status}</span>', unsafe_allow_html=True)
        st.markdown('<div class="section-note">Follow each file through the six real processing stages.</div>', unsafe_allow_html=True)
        metric = res["metrics"] if res else None
        cards = [
            ("Input", "file-up", "Load files and split into tasks", f"{metric['files_loaded']} files · {metric['mapper_tasks']} tasks" if res else "Waiting for input"),
            ("Map", "layers", "Count words in parallel", f"{metric['mapper_tasks']} tasks · {metric['mapper_workers']} workers" if res else "Local word counts"),
            ("Combine", "combine", "Merge mapper-local counts", f"{metric['local_combined_keys']:,} local keys" if res else "Reduce network data"),
            ("Partition", "shuffle", "Route each key to one reducer", f"{metric['partition_keys'][0]:,} + {metric['partition_keys'][1]:,} keys" if res else "Deterministic routing"),
            ("Reduce", "cpu", "Aggregate the assigned keys", f"{metric['reducer_unique_words'][0]:,} + {metric['reducer_unique_words'][1]:,} words" if res else "Two parallel reducers"),
            ("Output", "chart", "Merge and sort final counts", f"{metric['unique_words']:,} unique words" if res else "Alphabetical results"),
        ]
        stage_html = '<div class="stage-flow">' + "".join(
            f'<div class="stage" data-stage="{name}"><span class="stage-top"><span class="num">{i}</span>{icon_svg(icon, 21)}</span><span class="title">{name}</span><span class="desc">{desc}</span><span class="metric">{value}</span><span class="stage-status">{pipeline_status}</span></div>'
            for i, (name, icon, desc, value) in enumerate(cards, 1)) + '</div>'
        st.markdown(stage_html, unsafe_allow_html=True)
        if res:
            st.markdown("**Pipeline execution flow**")
            labels = ["Input", "Map", "Combine", "Partition", "Reduce", "Output"]
            for name in labels:
                st.markdown(f"<span class='status-pill'>{icon_svg('check', 14)} Completed</span> **{name}** &nbsp; <span class='section-note'>{res['timings'][name.lower()]*1000:.2f} ms</span>", unsafe_allow_html=True)
        else:
            st.info("Run the pipeline to see measured stage results and execution times.")
        with st.expander("What happens in each stage?"):
            st.markdown("""
            - **Input:** Uploaded text stays in memory and is divided into mapper tasks.
            - **Map:** ThreadPoolExecutor workers tokenize text and count words locally.
            - **Combine:** Local `Counter` results are merged to reduce duplicate keys.
            - **Partition:** `sum(map(ord, word)) % 2` routes each unique key to reducer 0 or 1.
            - **Reduce:** Exactly two worker tasks aggregate their assigned keys.
            - **Output:** Reducer results are merged, validated, and sorted alphabetically.
            """)

with right:
    with st.container(border=True):
        st.markdown(
            f"<div class='section-title'>{icon_svg('chart-pie', 21)}<span>3. Results & Insights</span></div>",
            unsafe_allow_html=True,
        )
        res = st.session_state.result
        if not res:
            st.markdown('<div class="empty-state"><strong>Your results will appear here</strong><br><span class="section-note">Upload text and run MapReduce to explore word counts.</span></div>', unsafe_allow_html=True)
        else:
            metrics = res["metrics"]
            summary = [("Files processed", metrics["files_loaded"]), ("Total words", f"{metrics['total_words']:,}"),
                       ("Unique words", f"{metrics['unique_words']:,}"), ("Mapper workers", metrics["mapper_workers"]),
                       ("Reducers", REDUCER_COUNT), ("Runtime", f"{res['timings']['total']:.4f} s")]
            for start in (0, 2, 4):
                cols = st.columns(2)
                for col, (label, value) in zip(cols, summary[start:start+2]):
                    col.markdown(f'<div class="summary-card"><div class="summary-label">{label}</div><div class="summary-value">{value}</div></div>', unsafe_allow_html=True)

            rows = [{"Word": word, "Count": count, "Frequency (%)": (100 * count / metrics["total_words"] if metrics["total_words"] else 0)}
                    for word, count in res["counts"].items()]
            dataframe = pd.DataFrame(rows, columns=["Word", "Count", "Frequency (%)"])
            st.markdown("#### Word frequency")
            search = st.text_input("Search words", placeholder="Type to filter…", label_visibility="collapsed")
            ordering = st.selectbox("Order", ["Highest frequency", "Alphabetical"], label_visibility="collapsed")
            filtered = dataframe[dataframe["Word"].str.contains(search, case=False, regex=False)] if search else dataframe
            filtered = filtered.sort_values("Word" if ordering == "Alphabetical" else "Count", ascending=ordering == "Alphabetical")
            st.dataframe(filtered, hide_index=True, width="stretch", height=230,
                         column_config={"Frequency (%)": st.column_config.NumberColumn(format="%.3f%%")})

            top_n = st.slider("Top N words", min_value=1, max_value=max(1, min(50, len(dataframe))), value=min(10, max(1, len(dataframe))))
            chart_data = dataframe.sort_values(["Count", "Word"], ascending=[False, True]).head(top_n).sort_values("Count")
            if not chart_data.empty:
                fig = px.bar(chart_data, x="Count", y="Word", orientation="h", color="Count",
                             color_continuous_scale=[PALETTE["mint"], PALETTE["periwinkle"], PALETTE["coral"]])
                fig.update_layout(height=270, margin=dict(l=0, r=0, t=12, b=0), showlegend=False,
                                  coloraxis_showscale=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

            output_text = "".join(f"{word} {count}\n" for word, count in res["counts"].items())
            with st.expander("Output preview · alphabetical"):
                st.code(output_text, language="text")
            csv_buffer = io.StringIO(newline="")
            writer = csv.writer(csv_buffer)
            writer.writerow(["word", "count"])
            writer.writerows(res["counts"].items())
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("output.txt", output_text)
                archive.writestr("output.csv", csv_buffer.getvalue())
                for index, counts in enumerate(res["reducer_counts"]):
                    archive.writestr(f"reducer_{index}.txt", "".join(f"{word} {count}\n" for word, count in sorted(counts.items())))
            downloads = st.columns(2)
            downloads[0].download_button("Download output.txt", output_text, file_name="output.txt", mime="text/plain", width="stretch")
            downloads[1].download_button("Download output.csv", csv_buffer.getvalue(), file_name="output.csv", mime="text/csv", width="stretch")
            d2 = st.columns(2)
            for index, counts in enumerate(res["reducer_counts"]):
                body = "".join(f"{word} {count}\n" for word, count in sorted(counts.items()))
                d2[index].download_button(f"Reducer {index} output", body, file_name=f"reducer_{index}.txt", mime="text/plain", width="stretch")
            st.download_button("Download all outputs (.zip)", zip_buffer.getvalue(), file_name="mapreduce_outputs.zip", mime="application/zip", width="stretch")
