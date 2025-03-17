import polars as pl
import pytest

from tests.data import TestDataframeRecord, test_dataframes
from timegroups.interfaces import DataFrameWithDatetimeIndex, PolarsDataFrame
from timegroups.time_group import TimeGroup


@pytest.mark.parametrize(
    "df, timestamp_col, time_groups",
    test_dataframes,
)
def test_get_time_groups_pandas(
    df: DataFrameWithDatetimeIndex,
    timestamp_col: str,  # noqa: ARG001
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import get_time_groups

    assert get_time_groups(df.index) == time_groups


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
def test_get_time_groups_polars(
    df: PolarsDataFrame,
    timestamp_col: str,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import get_time_groups

    assert get_time_groups(df.get_column(timestamp_col)) == time_groups
