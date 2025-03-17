from __future__ import annotations

from collections.abc import Iterable, Sequence, Sized
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar

if TYPE_CHECKING:
    from datetime import timedelta


C = TypeVar('C', covariant=True)


class Copyable(Protocol[C]):
    def __copy__(self) -> C: ...


class BooleanMaskProvider(Protocol[C]):
    def __bool__(self) -> bool: ...

    def __len__(self) -> int: ...

    def __getitem__(self, idx: int) -> bool: ...

    def __invert__(self) -> C: ...


BooleanMask = Sequence[bool] | BooleanMaskProvider


class IndexSeries(Protocol[C]):
    def __getitem__(self, key: slice | BooleanMask) -> Sequence[C]: ...

    def __len__(self) -> int: ...

    def __sub__(self, other: IndexSeries) -> Sequence[C]: ...


class Cloneable(Protocol[C]):
    def clone(self) -> C: ...


CopyOrCloneable = Copyable | Cloneable


class FrequencyProvider(Protocol):
    @property
    def freq(self) -> timedelta: ...


class Diffable(Protocol[C]):
    def diff(self, periods: int = 1, null_behavior: str | None = None) -> C: ...


class Filterable(Protocol[C]):
    def filter(self: C, predicate: Iterable[bool]) -> C: ...


class BooleanIndexable(Sized, Protocol[C]): ...


class RoundablePandas(Protocol[C]):
    def round(self, freq: timedelta) -> C: ...


class RoundablePolars(Protocol[C]):
    def round(self, every: timedelta) -> C: ...


class FilterableTimeseries(Diffable, Filterable, IndexSeries, Protocol[C]):
    @property
    def dt(self) -> RoundablePolars: ...


class DateTimeIndexLike(BooleanIndexable, Diffable, FrequencyProvider, IndexSeries, RoundablePandas, Protocol):
    def duplicated(self, keep: Literal["first", "last", False]) -> BooleanMaskProvider: ...


class Interpolatable(Protocol[C]):
    def interpolate(self) -> C: ...


class DataFrameWithDatetimeIndex(BooleanIndexable, Interpolatable, Protocol[C]):
    @property
    def index(self) -> DateTimeIndexLike: ...

    @index.setter
    def index(self, idx: DateTimeIndexLike) -> None: ...

    def asfreq(self, freq: timedelta) -> C: ...

    def __getitem__(self, key: Sequence[bool]) -> C: ...


class PolarsDataFrame(Interpolatable, Protocol[C]):
    def get_column(self, name: str) -> FilterableTimeseries: ...

    def get_column_index(self, name: str) -> int: ...

    def replace_column(self, index: int, column: FilterableTimeseries) -> None: ...

    def unique(self, subset: list[str], keep: str) -> C: ...

    def sort(self, by: str) -> C: ...

    def upsample(self, time_column: str, every: timedelta) -> C: ...
