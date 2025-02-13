from dataclasses import dataclass

import pandas as pd


@dataclass
class TimeGroup:
    """Dataclass to represent a time group."""

    start: pd.Timestamp
    end: pd.Timestamp

    @property
    def duration(self) -> pd.Timedelta:
        """Get the duration of the time group."""
        return self.end - self.start

    def __repr__(self) -> str:
        """Return a string representation of the TimeGroup."""
        return f"TimeGroup({self.start}, {self.end})"
