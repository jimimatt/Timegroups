from typing import NamedTuple

import pandas as pd

from timegroups.interfaces import DataFrameWithDatetimeIndex as PandasDataframe
from timegroups.interfaces import PolarsDataFrame
from timegroups.time_group import TimeGroup


class TestDataframeRecord(NamedTuple):
    input_df: PandasDataframe | PolarsDataFrame
    input_timestamp_col: str
    expected_time_groups: list[TimeGroup]


test_dataframes: list[TestDataframeRecord] = [
    TestDataframeRecord(
        input_df=pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            },
            index=pd.DatetimeIndex(pd.date_range("2022-01-01", periods=10, freq="D"), name="time"),
        ),
        input_timestamp_col="time",
        expected_time_groups=[TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-10"))],
    ),
    TestDataframeRecord(
        input_df=pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6],
            },
            index=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=1, freq="D").append(
                    pd.date_range("2022-01-08", periods=5, freq="D")
                ),
                name="time",
            ),
        ),
        input_timestamp_col="time",
        expected_time_groups=[
            TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-01")),
            TimeGroup(pd.Timestamp("2022-01-08"), pd.Timestamp("2022-01-12")),
        ],
    ),
    TestDataframeRecord(
        input_df=pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6],
            },
            index=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=5, freq="D").append(
                    pd.date_range("2022-01-08", periods=1, freq="D")
                ),
                name="time",
            ),
        ),
        input_timestamp_col="time",
        expected_time_groups=[
            TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-05")),
            TimeGroup(pd.Timestamp("2022-01-08"), pd.Timestamp("2022-01-8")),
        ],
    ),
    TestDataframeRecord(
        input_df=pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            },
            index=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=5, freq="D").append(
                    pd.date_range("2022-01-08", periods=1, freq="D").append(
                        pd.date_range("2022-01-12", periods=5, freq="D")
                    )
                ),
                name="time",
            ),
        ),
        input_timestamp_col="time",
        expected_time_groups=[
            TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-05")),
            TimeGroup(pd.Timestamp("2022-01-08"), pd.Timestamp("2022-01-08")),
            TimeGroup(pd.Timestamp("2022-01-12"), pd.Timestamp("2022-01-16")),
        ],
    ),
    TestDataframeRecord(
        input_df=pd.DataFrame(
            {
                "value": [],
            },
            index=pd.DatetimeIndex(pd.date_range("2022-01-01", periods=0, freq="D"), name="time"),
        ),
        input_timestamp_col="time",
        expected_time_groups=[],
    ),
]
