from datetime import datetime, timedelta, time

FIRST_SHIFT = time(hour=6)
SECOND_SHIFT = time(hour=14)
THIRD_SHIFT = time(hour=22)


def next_weekday(date: datetime, weekday: int) -> datetime:
    """
    Returns the next weekdays 1st shift start datetime
    @param date: current datetime
    @param weekday: day of the week: 0 - monday, 1 - tuesday, ..., 6 - sunday
    @return: next weekdays 1st shift start datetime
    """
    days_ahead = weekday - date.weekday()
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    return datetime.combine(date.date() + timedelta(days_ahead), time(hour=6))


def next_n_days(date: datetime, n: int) -> datetime:
    """
    Returns the next n days first shift datetime
    @param date: current date
    @param n: number of days
    @return: next n days first shift start date
    """
    if not n:
        return date
    return datetime.combine(date.date() + timedelta(days=n), time(hour=6))


def next_shift(date: datetime) -> datetime:
    """
    Returns the next shift start datetime
    @param date:
    @return:
    """
    if date.time() < FIRST_SHIFT:
        return datetime.combine(date.date(), FIRST_SHIFT)
    elif date.time() < SECOND_SHIFT:
        return datetime.combine(date.date(), SECOND_SHIFT)
    elif date.time() < THIRD_SHIFT:
        return datetime.combine(date.date(), THIRD_SHIFT)
    else:
        return datetime.combine(date.date() + timedelta(days=1), FIRST_SHIFT)
