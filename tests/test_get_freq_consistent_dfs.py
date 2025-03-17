from datetime import timedelta

import pandas as pd
import polars as pl
import pytest

from tests.data import TestDataframeRecord, test_dataframes
from timegroups.time_group import TimeGroup


@pytest.mark.parametrize(
    "df, timestamp_col, time_groups",
    test_dataframes,
)
def test_get_freq_consistent_dfs_pandas(
    df: pd.DataFrame,
    timestamp_col: str,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import get_freq_consistent_dfs

    dfs = get_freq_consistent_dfs(df, freq=timedelta(days=1), timestamp_column=timestamp_col)
    assert len(dfs) == len(time_groups)
    for tg, df in zip(time_groups, dfs, strict=False):
        assert df.index[0] == tg.start
        assert df.index[-1] == tg.end
        assert df.index.freq is not None
        assert df.isna().sum().sum() == 0


@pytest.mark.parametrize(
    "df, timestamp_col, time_groups",
    [
        TestDataframeRecord(
            input_df=pl.from_pandas(df, include_index=True),
            input_timestamp_col=timestamp_col,
            expected_time_groups=time_groups,
        )
        for df, timestamp_col, time_groups in test_dataframes
    ],
)
def test_get_freq_consistent_dfs_polars(
    df: pd.DataFrame,
    timestamp_col: str,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import get_freq_consistent_dfs

    dfs = get_freq_consistent_dfs(df, freq=timedelta(days=1), timestamp_column=timestamp_col)
    assert len(dfs) == len(time_groups)
    for tg, df in zip(time_groups, dfs, strict=False):
        assert df.get_column(timestamp_col)[0] == tg.start
        assert df.get_column(timestamp_col)[-1] == tg.end
        assert df.null_count().sum_horizontal()[0] == 0
