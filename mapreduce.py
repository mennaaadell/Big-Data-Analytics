"""Reusable, deterministic two-reducer MapReduce word-count backend."""

from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from time import perf_counter
import re
from typing import Mapping


REDUCER_COUNT = 2
TOKEN_PATTERN = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)


@dataclass
class MapReduceResult:
    counts: dict[str, int]
    reducer_counts: list[dict[str, int]]
    metrics: dict[str, object]
    timings: dict[str, float]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def partition(word: str) -> int:
    """Assign each word deterministically to exactly one of two reducers."""
    return sum(map(ord, word)) % 2


def tokenize(text: str) -> list[str]:
    """Lowercase Unicode words; numeric tokens are intentionally included."""
    return [match.group(0).lower() for match in TOKEN_PATTERN.finditer(text)]


def _make_chunks(files: Mapping[str, str], split_mode: str, lines_per_chunk: int) -> list[tuple[str, str]]:
    chunks: list[tuple[str, str]] = []
    for filename in sorted(files):
        text = files[filename]
        if split_mode == "By file":
            chunks.append((filename, text))
            continue
        lines = text.splitlines(keepends=True)
        if not lines:
            chunks.append((filename, ""))
        else:
            for start in range(0, len(lines), lines_per_chunk):
                chunks.append((filename, "".join(lines[start:start + lines_per_chunk])))
    return chunks


def run_mapreduce(
    files: Mapping[str, str], mapper_workers: int = 4, *,
    split_mode: str = "By lines", lines_per_chunk: int = 500,
) -> MapReduceResult:
    """Run Input → Map → Combine → Partition → Reduce → Output in memory.

    Mapper tasks build local Counters. Each local key is sent to one reducer
    using the preserved assignment partition function, and two reducer tasks
    aggregate their own partitions concurrently.
    """
    if not files:
        raise ValueError("At least one decoded input file is required.")
    if mapper_workers < 1:
        raise ValueError("Mapper worker count must be at least 1.")
    if lines_per_chunk < 1:
        raise ValueError("Lines per chunk must be at least 1.")

    overall_start = perf_counter()
    stage_times: dict[str, float] = {}

    stage_start = perf_counter()
    chunks = _make_chunks(files, split_mode, lines_per_chunk)
    input_tokens = sum(len(tokenize(text)) for text in files.values())
    stage_times["input"] = perf_counter() - stage_start

    stage_start = perf_counter()
    with ThreadPoolExecutor(max_workers=mapper_workers, thread_name_prefix="mapper") as pool:
        local_counters = list(pool.map(lambda pair: Counter(tokenize(pair[1])), chunks))
    stage_times["map"] = perf_counter() - stage_start

    stage_start = perf_counter()
    combined = Counter()
    for local in local_counters:
        combined.update(local)
    stage_times["combine"] = perf_counter() - stage_start

    stage_start = perf_counter()
    partitions: list[dict[str, int]] = [{}, {}]
    for word, count in combined.items():
        partitions[partition(word)][word] = count
    stage_times["partition"] = perf_counter() - stage_start

    stage_start = perf_counter()
    with ThreadPoolExecutor(max_workers=REDUCER_COUNT, thread_name_prefix="reducer") as pool:
        reduced = list(pool.map(lambda part: dict(sorted(part.items())), partitions))
    stage_times["reduce"] = perf_counter() - stage_start

    stage_start = perf_counter()
    merged = dict(sorted({word: count for part in reduced for word, count in part.items()}.items()))
    stage_times["output"] = perf_counter() - stage_start
    stage_times["total"] = perf_counter() - overall_start

    if sum(merged.values()) != input_tokens:
        raise RuntimeError("Validation failed: reduced counts do not equal the input token count.")
    if len(merged) != sum(len(part) for part in reduced):
        raise RuntimeError("Validation failed: a word appeared in more than one reducer output.")
    if any(partition(word) != index for index, part in enumerate(reduced) for word in part):
        raise RuntimeError("Validation failed: a reducer received a word assigned to another partition.")

    metrics: dict[str, object] = {
        "files_loaded": len(files),
        "mapper_tasks": len(chunks),
        "mapper_workers": mapper_workers,
        "local_combined_keys": sum(len(counter) for counter in local_counters),
        "global_combined_keys": len(combined),
        "partition_keys": [len(part) for part in partitions],
        "reducer_unique_words": [len(part) for part in reduced],
        "total_words": input_tokens,
        "unique_words": len(merged),
        "reducer_count": REDUCER_COUNT,
        "tokenization": "Unicode words; lowercased; numbers included",
    }
    return MapReduceResult(merged, reduced, metrics, stage_times)
