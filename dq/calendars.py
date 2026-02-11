from __future__ import annotations

from datetime import date
import pandas as pd
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    Holiday,
    USMartinLutherKingJr,
    USPresidentsDay,
    USMemorialDay,
    USLaborDay,
    USThanksgivingDay,
    nearest_workday,
    GoodFriday,
    Easter,
)
from pandas.tseries.offsets import CustomBusinessDay, Day


# Reasonable NYSE-like holiday set (good enough for VaR DQ demo)
# (Excludes Columbus/Veterans because equities trade those days)
class NYSEHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("NewYearsDay", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday("Juneteenth", month=6, day=19, observance=nearest_workday),
        Holiday("IndependenceDay", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("ChristmasDay", month=12, day=25, observance=nearest_workday),
    ]


class USTreasuryHolidayCalendar(AbstractHolidayCalendar):
    # Practical “good-enough” SIFMA-like holiday set for a VaR DQ demo.
    # (Avoids Columbus/Veterans to not under-flag; still kills the obvious false positives.)
    rules = [
        Holiday("NewYearsDay", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday("Juneteenth", month=6, day=19, observance=nearest_workday),
        Holiday("IndependenceDay", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("ChristmasDay", month=12, day=25, observance=nearest_workday),
    ]


# TARGET-ish calendar for ECB FX (not perfect, but removes obvious false flags)
class TARGETLikeCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("NewYearsDay", month=1, day=1, observance=nearest_workday),
        GoodFriday,
        Holiday("EasterMonday", month=1, day=1, offset=[Easter(), Day(1)]),
        Holiday("LabourDay", month=5, day=1),
        Holiday("ChristmasDay", month=12, day=25),
        Holiday("BoxingDay", month=12, day=26),
    ]


def expected_dates(asset_class: str, start: date, end: date) -> set[date]:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    if asset_class == "equities":
        hol = NYSEHolidayCalendar().holidays(start_ts, end_ts)
        cbd = CustomBusinessDay(holidays=hol)
        return set(pd.date_range(start_ts, end_ts, freq=cbd).date)

    if asset_class == "rates":
        hol = USTreasuryHolidayCalendar().holidays(start_ts, end_ts)
        cbd = CustomBusinessDay(holidays=hol)
        return set(pd.date_range(start_ts, end_ts, freq=cbd).date)

    if asset_class == "fx":
        hol = TARGETLikeCalendar().holidays(start_ts, end_ts)
        cbd = CustomBusinessDay(holidays=hol)
        return set(pd.date_range(start_ts, end_ts, freq=cbd).date)

    # commodities fallback: weekdays (avoid overengineering for demo)
    return set(pd.bdate_range(start_ts, end_ts).date)
