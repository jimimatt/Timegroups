from datetime import timedelta
from typing import NamedTuple

import pandas as pd
import polars as pl
import pytest

from timegroups.interfaces import IndexSeries


class TestDataRecord(NamedTuple):
    input_data: IndexSeries
    expected_outcome: timedelta


def get_test_guess_freq_data() -> list[TestDataRecord]:
    return [
        TestDataRecord(
            input_data=pd.DatetimeIndex(pd.date_range("2022-01-01", periods=10, freq="D")),
            expected_outcome=timedelta(days=1),
        ),
        TestDataRecord(
            input_data=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=1, freq="D").append(
                    pd.date_range("2022-01-08", periods=5, freq="D")
                )
            ),
            expected_outcome=timedelta(days=1),
        ),
        TestDataRecord(
            input_data=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=5, freq="D").append(
                    pd.date_range("2022-01-08", periods=1, freq="D").append(
                        pd.date_range("2022-01-12", periods=5, freq="D")
                    )
                )
            ),
            expected_outcome=timedelta(days=1),
        ),
    ]


@pytest.mark.parametrize(
    "idx, dt",
    get_test_guess_freq_data()
    + [TestDataRecord(input_data=pl.from_pandas(idx), expected_outcome=dt) for idx, dt in get_test_guess_freq_data()],
)
def test_guess_freq(
    idx: IndexSeries,
    dt: timedelta,
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import guess_freq

    assert guess_freq(idx) == dt
