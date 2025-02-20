from collections import Counter
from typing import Literal

import numpy as np
import pandas as pd

from timegroups.time_group import TimeGroup


def guess_freq(idx: pd.DatetimeIndex) -> pd.Timedelta:
    """Guess the frequency of a DatetimeIndex."""
    if len(idx) < 2:
        raise ValueError("Could not guess frequency. Index must have at least two elements.")
    idx_diff = idx[1:] - idx[:-1]
    most_common_freq, _ = Counter[pd.Timedelta](idx_diff).most_common(1)[0]
    return most_common_freq


def get_time_groups(
    idx: pd.DatetimeIndex, time_delta_factor: float = 2.0, freq: pd.Timedelta | None = None
) -> list[TimeGroup]:
    """Get time groups from a DatetimeIndex.

    Args:
        idx (pd.DatetimeIndex): DatetimeIndex to get time groups from.
        time_delta_factor (float): Determine the gap (in combination with `freq`)
         that still will be considered all-over.
        freq (pd.Timedelta): Frequency of the DatetimeIndex.

    Returns:
        list[TimeGroup]: List of TimeGroups.
    """
    if len(idx) < 1:
        return []
    if freq is None:
        freq = idx.freq if idx.freq is not None else guess_freq(idx)  # type: ignore
    idx_diff = idx[1:] - idx[:-1]

    mask = idx_diff > time_delta_factor * freq  # type: ignore
    begin_mask = np.append([True], mask)
    end_mask = np.append(mask, [True])

    tgs_begins = idx[begin_mask]
    tgs_ends = idx[end_mask]

    return [TimeGroup(t_start, t_end) for t_start, t_end in (zip(tgs_begins, tgs_ends, strict=False))]


def split_df_by_tgs(df: pd.DataFrame, tgs: list[TimeGroup]) -> list[pd.DataFrame]:
    """Split a DataFrame by TimeGroups."""
    return [df.loc[tg.start : tg.end] for tg in tgs]


def align_datetime_index(
    df: pd.DataFrame, freq: pd.Timedelta | pd.DateOffset | str, duplicates: Literal["silent", "error"] = "silent"
) -> pd.DataFrame:
    """Align the DatetimeIndex of a DataFrame to a frequency.

    Args:
        df (pd.DataFrame): DataFrame to align.
        freq (pd.Timedelta | pd.DateOffset | str): Frequency to align to.
        duplicates (Literal["silent", "error"], optional): How to handle duplicates. Defaults to "silent".

    Returns:
        pd.DataFrame: Aligned DataFrame.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Index must be a DatetimeIndex.")
    df.index = df.index.round(freq=freq)  # type: ignore
    if duplicates == "silent":
        df = df.drop_duplicates(keep="first")
    else:
        if df.index.duplicated().any():
            raise ValueError("Index contains duplicates.")
    return df.asfreq(freq)  # type: ignore


def get_freq_consistent_dfs(df: pd.DataFrame, freq: pd.Timedelta, time_delta_factor: float = 2.0) -> list[pd.DataFrame]:
    """Get DataFrames with consistent frequency.

    Args:
        df (pd.DataFrame): DataFrame to split.
        freq (pd.Timedelta): Frequency to split by.
        time_delta_factor (float, optional): Determine the gap that still will be considered all-over. Defaults to 2.0.

    Returns:
        list[pd.DataFrame]: List of DataFrames with consistent frequency.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Index must be a DatetimeIndex.")
    dfs: list[pd.DataFrame] = []
    tgs = get_time_groups(df.index, freq=freq, time_delta_factor=time_delta_factor)
    for tg in tgs:
        sub_df = df.loc[tg.start : tg.end]
        sub_df = align_datetime_index(sub_df, freq)
        sub_df = sub_df.interpolate("linear")
        dfs.append(sub_df)
    return dfs
