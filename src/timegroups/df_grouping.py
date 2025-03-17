from collections import Counter
from datetime import timedelta
from typing import Literal, TypeVar, cast

from timegroups.interfaces import DataFrameWithDatetimeIndex as PandasDataframe
from timegroups.interfaces import (
    DateTimeIndexLike,
    FilterableTimeseries,
    IndexSeries,
    PolarsDataFrame,
)
from timegroups.time_group import TimeGroup

T = TypeVar('T')
S = TypeVar('S', bound=PandasDataframe | PolarsDataFrame)


def guess_freq(idx: IndexSeries) -> timedelta:
    """Guess the frequency of a Timestamp Series."""
    if len(idx) < 2:
        raise ValueError("Could not guess frequency. Index must have at least two elements.")
    idx_diff = cast(IndexSeries, idx[1:]) - cast(IndexSeries, idx[:-1])
    most_common_freq, _ = Counter[timedelta](idx_diff).most_common(1)[0]
    return most_common_freq


def get_time_groups(
    idx: FilterableTimeseries | DateTimeIndexLike,
    time_delta_factor: float = 2.0,
    freq: timedelta | None = None,
) -> list[TimeGroup]:
    """Get time groups from a `FilterableTimeseries` or `DateTimeIndexLike`.

    Args:
        idx (`FilterableTimeseries | DateTimeIndexLike`): Can be pd.DatetimeIndex / pd.Series / pl.Series.
         Base to derive time groups on.
        time_delta_factor (float): Determine the gap (in combination with `freq`)
         that still will be considered all-over.
        freq (timedelta): Frequency of the idx.

    Returns:
        list[TimeGroup]: List of TimeGroups.
    """
    if len(idx) < 1:
        return []
    if freq is None:
        freq = idx.freq if hasattr(idx, 'freq') and idx.freq is not None else guess_freq(idx)

    idx_diff = idx.diff()
    mask = idx_diff > time_delta_factor * freq
    mask[0] = True
    begin_mask = mask

    if hasattr(mask, 'clone'):
        end_mask = mask.clone()
        end_mask = end_mask.shift(-1)
        end_mask[-1] = True
    else:
        end_mask = mask.copy()
        end_mask[:-1] = mask[1:]
        end_mask[-1] = True

    if hasattr(idx, 'filter'):
        tgs_begins = idx.filter(begin_mask)
        tgs_ends = idx.filter(end_mask)
    else:
        tgs_begins = idx[begin_mask]
        tgs_ends = idx[end_mask]

    return [TimeGroup(t_start, t_end) for t_start, t_end in (zip(tgs_begins, tgs_ends, strict=False))]


def split_df_by_tgs(df: T, tgs: list[TimeGroup], timestamp_column: str | None = None) -> list[T]:
    """Split a DataFrame by TimeGroups."""
    if hasattr(df, 'filter') and hasattr(df, 'get_column'):
        if timestamp_column is None:
            raise ValueError("timestamp_column must be provided.")
        time_col = df.get_column(timestamp_column)
        return [df.filter(time_col.is_between(tg.start, tg.end, closed="both")) for tg in tgs]
    elif hasattr(df, 'loc') and hasattr(df, 'index'):
        return [df.loc[tg.start : tg.end] for tg in tgs]
    raise NotImplementedError(
        "DataFrame must have either 'filter' (and 'timestamp_column' must be provided)  or 'loc' method (plus 'index')."
    )


def align_datetime_pandas(
    df: PandasDataframe, freq: timedelta, keep_duplicates: Literal["first", "last", False] = "first"
) -> PandasDataframe:
    """Align the DatetimeIndex of a DataFrame to a frequency.

    Args:
        df (PandasDataframe): DataFrame to align.
        freq (timedelta): Frequency to align to.
        keep_duplicates (Literal["first", "last"] | False): How to handle duplicates. Defaults to "silent".

    Returns:
        PandasDataframe: Aligned DataFrame.
    """
    df.index = df.index.round(freq)
    df = df[~df.index.duplicated(keep=keep_duplicates)]
    return df.asfreq(freq)


def align_datetime_polars(
    df: PolarsDataFrame,
    freq: timedelta,
    timestamp_column: str,
    keep_duplicates: Literal["first", "last", "any", "none"] = "first",
) -> PolarsDataFrame:
    """Align the Timestamp column of a DataFrame to a frequency.

    Args:
        df (PolarsDataFrame): DataFrame to align.
        freq (timedelta): Frequency to align to.
        timestamp_column (str): Name of the timestamp column.
        keep_duplicates (Literal["first", "last", "any", "none"], optional): How to handle duplicates.
         Defaults to "first".

    Returns:
        PolarsDataFrame: Aligned DataFrame.
    """
    aligned_time_col = df.get_column(timestamp_column).dt.round(freq)
    df.replace_column(index=df.get_column_index(timestamp_column), column=aligned_time_col)
    df = df.unique(subset=[timestamp_column], keep=keep_duplicates)
    return df.sort(timestamp_column).upsample(time_column=timestamp_column, every=freq)  # TODO: Investigate shuffle


def align_datetime(
    df: S,
    freq: timedelta,
    timestamp_column: str | None,
    duplicates: Literal["silent", "error"] = "silent",
) -> S:
    """Align the DatetimeIndex or a timestamp column of a DataFrame to a frequency."""
    if hasattr(df, "index"):
        keep_duplicates_pd: Literal["first", "last", False] = "first"
        if duplicates == "error":
            keep_duplicates_pd = False
        return cast(S, align_datetime_pandas(cast(PandasDataframe, df), freq, keep_duplicates=keep_duplicates_pd))
    elif hasattr(df, "get_column") and timestamp_column is not None:
        keep_duplicates_pl: Literal["first", "last", "any", "none"] = "first"
        if duplicates == "error":
            keep_duplicates_pl = "any"
        return cast(
            S,
            align_datetime_polars(
                cast(PolarsDataFrame, df), freq, timestamp_column, keep_duplicates=keep_duplicates_pl
            ),
        )
    raise ValueError("DataFrame must have either 'index' or 'get_column' method and timestamp_column must not be None.")


def get_freq_consistent_dfs(
    df: S,
    freq: timedelta,
    time_delta_factor: float = 2.0,
    timestamp_column: str | None = None,
    duplicates: Literal["silent", "error"] = "silent",
) -> list[S]:
    """Get DataFrames with consistent frequency.

    Args:
        df (S): DataFrame to split.
        freq (timedelta): Frequency to split by.
        time_delta_factor (float, optional): Determine the gap that still will be considered all-over. Defaults to 2.0.
        timestamp_column (str, optional): Name of the timestamp column. Defaults to None.
        duplicates (Literal["silent", "error"], optional): How to handle duplicates. Defaults to "silent".

    Returns:
        list[S]: List of DataFrames with consistent frequency.
    """
    if hasattr(df, 'index'):
        datetime_series = df.index
    elif hasattr(df, 'get_column') and timestamp_column is not None:
        datetime_series = df.get_column(timestamp_column)
    else:
        raise ValueError(
            "DataFrame must have either 'index' or 'get_column' method and timestamp_column must not be None."
        )
    tgs = get_time_groups(datetime_series, freq=freq, time_delta_factor=time_delta_factor)
    dfs: list[S] = split_df_by_tgs(df, tgs, timestamp_column=timestamp_column)
    for i in range(len(dfs)):
        sub_df = align_datetime(dfs[i], freq, timestamp_column=timestamp_column, duplicates=duplicates)
        dfs[i] = sub_df.interpolate()
    return dfs
