# xphi.bound.xor.parser.mark
## @lineage: xphi.bound.parser.mark
## @lineage: xphi.xor.parser.mark
from __future__ import annotations

import copy
import datetime
import hashlib
import itertools
import os
import warnings
from collections import defaultdict
from collections.abc import Callable
from datetime import date
from functools import cache
from importlib.metadata import PackageNotFoundError, version as get_version
from pathlib import Path
from typing import Any, TypeVar, cast

import requests
import tqdm
from deprecation import (
    DeprecatedWarning,
    UnsupportedWarning,
    deprecated as _deprecated,
)
from packaging import version as pkg_version

from xphi.watcher.plane.emitter import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------
# Constants & TypeVars
# ---------------------------------------------------------
_FuncT = TypeVar("_FuncT", bound=Callable[..., Any])

DEFAULT_TEXT_CONTENT_LIMIT = 50_000
DEFAULT_TRUNCATE_NOTICE = (
    "<response clipped><NOTE>Due to the max output limit, only part of the full "
    "response has been shown to you.</NOTE>"
)
DEFAULT_TRUNCATE_NOTICE_WITH_PERSIST = (
    "<response clipped><NOTE>Due to the max output limit, only part of the full "
    "response has been shown to you. The complete output has been saved to "
    "{file_path} - you can use other tools to view the full content (truncated "
    "part starts around line {line_num}).</NOTE>"
)


# =========================================================
# Deprecation & Cleanup Utilities (formerly in depre.py)
# =========================================================

@cache
def _current_version() -> str:
    try:
        return get_version("no-version")
    except PackageNotFoundError:
        return "0.0.0"


def deprecated(
    *,
    deprecated_in: str,
    removed_in: str | date | None,
    current_version: str | None = None,
    details: str = "",
) -> Callable[[_FuncT], _FuncT]:
    base_decorator = _deprecated(
        deprecated_in=deprecated_in,
        removed_in=removed_in,
        current_version=current_version or _current_version(),
        details=details,
    )

    def decorator(func: _FuncT) -> _FuncT:
        return cast(_FuncT, base_decorator(func))

    return decorator


def _should_warn(
    *,
    deprecated_in: str | None,
    removed_in: str | date | None,
    current_version: str | None,
) -> tuple[bool, bool]:
    is_deprecated = False
    is_unsupported = False

    if isinstance(removed_in, date):
        if date.today() >= removed_in:
            is_unsupported = True
        else:
            is_deprecated = True
    elif current_version:
        current = pkg_version.parse(current_version)
        if removed_in and current >= pkg_version.parse(str(removed_in)):
            is_unsupported = True
        elif deprecated_in and current >= pkg_version.parse(deprecated_in):
            is_deprecated = True
    else:
        is_deprecated = True

    return is_deprecated, is_unsupported


def warn_deprecated(
    feature: str,
    *,
    deprecated_in: str,
    removed_in: str | date | None,
    current_version: str | None = None,
    details: str = "",
    stacklevel: int = 2,
) -> None:
    current_version = current_version or _current_version()
    is_deprecated, is_unsupported = _should_warn(
        deprecated_in=deprecated_in,
        removed_in=removed_in,
        current_version=current_version,
    )

    if not (is_deprecated or is_unsupported):
        return

    warning_cls = UnsupportedWarning if is_unsupported else DeprecatedWarning
    warning = warning_cls(feature, deprecated_in, removed_in, details)
    warnings.warn(warning, stacklevel=stacklevel)


def warn_cleanup(
    workaround: str,
    *,
    cleanup_by: str | date,
    current_version: str | None = None,
    details: str = "",
    stacklevel: int = 2,
) -> None:
    current_version = current_version or _current_version()
    should_cleanup = False
    if isinstance(cleanup_by, date):
        should_cleanup = date.today() >= cleanup_by
    else:
        try:
            current = pkg_version.parse(current_version)
            target = pkg_version.parse(str(cleanup_by))
            should_cleanup = current >= target
        except pkg_version.InvalidVersion:
            pass

    if should_cleanup:
        message = (
            f"Cleanup required: {workaround}. "
            f"This workaround was scheduled for removal by {cleanup_by}."
        )
        if details:
            message += f" {details}"
        warnings.warn(message, UserWarning, stacklevel=stacklevel)


def handle_deprecated_model_fields(
    data: Any,
    deprecated_fields: tuple[str, ...],
) -> Any:
    if not isinstance(data, dict):
        return data

    for field in deprecated_fields:
        data.pop(field, None)

    return data


# =========================================================
# Content Truncation Utilities (formerly in truncate.py)
# =========================================================

def _save_full_content(content: str, save_dir: str, tool_prefix: str) -> str | None:
    """Save full content to the specified directory and return the file path."""

    save_dir_path = Path(save_dir)
    save_dir_path.mkdir(parents=True, exist_ok=True)
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]
    filename = f"{tool_prefix}_output_{content_hash}.txt"
    file_path = save_dir_path / filename
    if not file_path.exists():
        try:
            file_path.write_text(content, encoding="utf-8")
        except Exception as e:
            logger.debug(f"Failed to save full content to {file_path}: {e}")
            return None

    return str(file_path)


def maybe_truncate(
    content: str,
    truncate_after: int | None = None,
    truncate_notice: str = DEFAULT_TRUNCATE_NOTICE,
    save_dir: str | None = None,
    tool_prefix: str = "output",
) -> str:
    """Truncate the middle of content if it exceeds the specified length"""
    if not truncate_after or len(content) <= truncate_after or truncate_after < 0:
        return content

    if len(truncate_notice) >= truncate_after:
        return truncate_notice[:truncate_after]

    available_chars = truncate_after - len(truncate_notice)
    proposed_head = available_chars // 2 + (available_chars % 2)
    final_notice = truncate_notice
    if save_dir:
        saved_file_path = _save_full_content(content, save_dir, tool_prefix)
        if saved_file_path:
            head_content_lines = len(content[:proposed_head].splitlines())
            final_notice = DEFAULT_TRUNCATE_NOTICE_WITH_PERSIST.format(
                file_path=saved_file_path,
                line_num=head_content_lines + 1,  # +1 to indicate next line
            )

    if len(final_notice) >= truncate_after:
        return final_notice[:truncate_after]

    remaining = truncate_after - len(final_notice)
    head_chars = min(proposed_head, remaining)
    tail_chars = remaining - head_chars
    return (
        content[:head_chars]
        + final_notice
        + (content[-tail_chars:] if tail_chars > 0 else "")
    )


# =========================================================
# General Utilities (formerly in depre.py)
# =========================================================

def download(url):
    filename = os.path.basename(url)
    remote_size = int(requests.head(url, allow_redirects=True).headers.get("Content-Length", 0))
    local_size = os.path.getsize(filename) if os.path.exists(filename) else 0

    if not os.path.exists(filename) or local_size != remote_size:
        print(f"Downloading '{filename}'...")
        with requests.get(url, stream=True) as r, open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def print_message(*s, condition=True, pad=False, sep=None):
    s = " ".join([str(x) for x in s])
    msg = "[{}] {}".format(datetime.datetime.now().strftime("%b %d, %H:%M:%S"), s)
    if condition:
        msg = msg if not pad else f"\n{msg}\n"
        print(msg, flush=True, sep=sep)
    return msg


def timestamp(daydir=False):
    format_str = f"%Y-%m{'/' if daydir else '-'}%d{'/' if daydir else '_'}%H.%M.%S"
    result = datetime.datetime.now().strftime(format_str)
    return result


def file_tqdm(file):
    print(f"#> Reading {file.name}")
    with tqdm.tqdm(
        total=os.path.getsize(file.name) / 1024.0 / 1024.0,
        unit="MiB",
    ) as pbar:
        for line in file:
            yield line
            pbar.update(len(line) / 1024.0 / 1024.0)

        pbar.close()


def create_directory(path):
    if os.path.exists(path):
        print("\n")
        print_message("#> Note: Output directory", path, "already exists\n\n")
    else:
        print("\n")
        print_message("#> Creating directory", path, "\n\n")
        os.makedirs(path)


def deduplicate(seq: list[str]) -> list[str]:
    return list(dict.fromkeys(seq))


def batch(group, bsize, provide_offset=False):
    offset = 0
    while offset < len(group):
        batch_data = group[offset : offset + bsize]
        yield ((offset, batch_data) if provide_offset else batch_data)
        offset += len(batch_data)
    return


class dotdict(dict):  # noqa: N801
    def __getattr__(self, key):
        if key.startswith("__") and key.endswith("__"):
            return super().__getattr__(key)
        try:
            return self[key]
        except KeyError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        if key.startswith("__") and key.endswith("__"):
            super().__setattr__(key, value)
        else:
            self[key] = value

    def __delattr__(self, key):
        if key.startswith("__") and key.endswith("__"):
            super().__delattr__(key)
        else:
            del self[key]

    def __deepcopy__(self, memo):
        # Use the default dict copying method to avoid infinite recursion.
        return dotdict(copy.deepcopy(dict(self), memo))


class dotdict_lax(dict):  # noqa: N801
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def flatten(data_list):
    result = []
    for child_list in data_list:
        result += child_list
    return result


def zipstar(data_list, lazy=False):
    """
    A much faster A, B, C = zip(*[(a, b, c), (a, b, c), ...])
    May return lists or tuples.
    """
    if len(data_list) == 0:
        return data_list

    width = len(data_list[0])
    if width < 100:
        return [[elem[idx] for elem in data_list] for idx in range(width)]

    zipped_data = zip(*data_list, strict=False)
    return zipped_data if lazy else list(zipped_data)


def zip_first(list1, list2):
    length = len(list1) if type(list1) in [tuple, list] else None
    zipped_data = list(zip(list1, list2, strict=False))
    assert length in [None, len(zipped_data)], "zip_first() failure: length differs!"
    return zipped_data


def int_or_float(val):
    if "." in val:
        return float(val)
    return int(val)


def groupby_first_item(lst):
    groups = defaultdict(list)
    for first, *rest in lst:
        rest = rest[0] if len(rest) == 1 else rest
        groups[first].append(rest)

    return groups


def process_grouped_by_first_item(lst):
    """Requires items in list to already be grouped by first item"""
    groups = defaultdict(list)

    started = False
    last_group = None
    for first, *rest in lst:
        rest = rest[0] if len(rest) == 1 else rest
        if started and first != last_group:
            yield (last_group, groups[last_group])
            assert first not in groups, f"{first} seen earlier --- violates precondition."

        groups[first].append(rest)

        last_group = first
        started = True

    return groups


def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks
        Example: grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
        Source: https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def lengths2offsets(lengths):
    offset = 0
    for length in lengths:
        yield (offset, offset + length)
        offset += length
    return


class NullContextManager:
    # see https://stackoverflow.com/a/45187287
    def __init__(self, dummy_resource=None):
        self.dummy_resource = dummy_resource

    def __enter__(self):
        return self.dummy_resource

    def __exit__(self, *args):
        pass


def load_batch_backgrounds(args, qids):
    if args.qid2backgrounds is None:
        return None

    qbackgrounds = []
    for qid in qids:
        back = args.qid2backgrounds[qid]
        if len(back) and isinstance(back[0], int):
            x = [args.collection[pid] for pid in back]
        else:
            x = [args.collectionX.get(pid, "") for pid in back]

        x = " [SEP] ".join(x)
        qbackgrounds.append(x)

    return qbackgrounds