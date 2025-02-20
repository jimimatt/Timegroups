import pandas as pd
import pytest

from timegroups.time_group import TimeGroup

test_dataframes: list[tuple[pd.DataFrame, list[TimeGroup]]] = [
    (
        pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            },
            index=pd.DatetimeIndex(pd.date_range("2022-01-01", periods=10, freq="D")),
        ),
        [TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-10"))],
    ),
    (
        pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6],
            },
            index=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=1, freq="D").append(
                    pd.date_range("2022-01-08", periods=5, freq="D")
                )
            ),
        ),
        [
            TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-01")),
            TimeGroup(pd.Timestamp("2022-01-08"), pd.Timestamp("2022-01-12")),
        ],
    ),
    (
        pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6],
            },
            index=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=5, freq="D").append(
                    pd.date_range("2022-01-08", periods=1, freq="D")
                )
            ),
        ),
        [
            TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-05")),
            TimeGroup(pd.Timestamp("2022-01-08"), pd.Timestamp("2022-01-8")),
        ],
    ),
    (
        pd.DataFrame(
            {
                "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            },
            index=pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=5, freq="D").append(
                    pd.date_range("2022-01-08", periods=1, freq="D").append(
                        pd.date_range("2022-01-12", periods=5, freq="D")
                    )
                )
            ),
        ),
        [
            TimeGroup(pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-05")),
            TimeGroup(pd.Timestamp("2022-01-08"), pd.Timestamp("2022-01-08")),
            TimeGroup(pd.Timestamp("2022-01-12"), pd.Timestamp("2022-01-16")),
        ],
    ),
    (
        pd.DataFrame(
            {
                "value": [],
            },
            index=pd.DatetimeIndex(pd.date_range("2022-01-01", periods=0, freq="D")),
        ),
        [],
    ),
]


@pytest.mark.parametrize(
    "idx, dt",
    [
        (
            pd.DatetimeIndex(pd.date_range("2022-01-01", periods=10, freq="D")),
            pd.Timedelta("1D"),
        ),
        (
            pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=1, freq="D").append(
                    pd.date_range("2022-01-08", periods=5, freq="D")
                )
            ),
            pd.Timedelta("1D"),
        ),
        (
            pd.DatetimeIndex(
                pd.date_range("2022-01-01", periods=5, freq="D").append(
                    pd.date_range("2022-01-08", periods=1, freq="D").append(
                        pd.date_range("2022-01-12", periods=5, freq="D")
                    )
                )
            ),
            pd.Timedelta("1D"),
        ),
    ],
)
def test_guess_freq(
    idx: pd.DatetimeIndex,
    dt: pd.Timedelta,
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import guess_freq

    assert guess_freq(idx) == dt


@pytest.mark.parametrize(
    "df, time_groups",
    test_dataframes,
)
def test_get_time_groups(
    df: pd.DataFrame,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import get_time_groups

    assert get_time_groups(df.index) == time_groups


@pytest.mark.parametrize(
    "df, time_groups",
    test_dataframes,
)
def test_split_df_by_tgs(
    df: pd.DataFrame,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import split_df_by_tgs

    dfs = split_df_by_tgs(df, time_groups)
    assert len(dfs) == len(time_groups)
    for i, tg in enumerate(time_groups):
        assert dfs[i].index[0] == tg.start
        assert dfs[i].index[-1] == tg.end


@pytest.mark.parametrize(
    "df, time_groups",
    test_dataframes,
)
def test_get_freq_consistent_dfs(
    df: pd.DataFrame,
    time_groups: list[TimeGroup],
) -> None:
    """Test get_time_groups method."""
    from timegroups.df_grouping import get_freq_consistent_dfs

    dfs = get_freq_consistent_dfs(df, pd.Timedelta("1D"))
    assert len(dfs) == len(time_groups)
    for tg, df in zip(time_groups, dfs, strict=False):
        assert df.index[0] == tg.start
        assert df.index[-1] == tg.end
        assert df.index.freq is not None
        assert df.isna().sum().sum() == 0
