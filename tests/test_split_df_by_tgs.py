import pandas as pd
import polars as pl
import pytest

from tests.data import TestDataframeRecord, test_dataframes
from timegroups.time_group import TimeGroup


@pytest.mark.parametrize(
    "df, timestamp_col, time_groups",
    test_dataframes,
)
def test_split_df_by_tgs_pandas(
    df: pd.DataFrame,
    timestamp_col: str,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import split_df_by_tgs

    dfs = split_df_by_tgs(df, time_groups, timestamp_col)
    assert len(dfs) == len(time_groups)
    for i, tg in enumerate(time_groups):
        assert dfs[i].index[0] == tg.start
        assert dfs[i].index[-1] == tg.end


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
def test_split_df_by_tgs_polars(
    df: pd.DataFrame,
    timestamp_col: str,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import split_df_by_tgs

    dfs = split_df_by_tgs(df, time_groups, timestamp_col)
    assert len(dfs) == len(time_groups)
    for i, tg in enumerate(time_groups):
        assert dfs[i].get_column(timestamp_col)[0] == tg.start
        assert dfs[i].get_column(timestamp_col)[-1] == tg.end
