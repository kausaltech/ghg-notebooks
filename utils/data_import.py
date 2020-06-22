from datetime import timedelta, date


MIN_DATE = date(1950, 1, 1)


def find_data_start(check_func, max_date=None, min_date=None):
    """Bisect date ranges to find the first date with valid data"""
    if not max_date:
        max_date = date.today()

    if min_date:
        td = max_date - min_date
    else:
        td = timedelta(days=1 << 12)
        min_date = MIN_DATE
    found_bad = False
    last_good = None
    last_bad = None

    check_date = max_date - td
    last_bad = check_date
    last_good = max_date

    td /= 2
    while (last_good - last_bad) != timedelta(days=1):
        if td < timedelta(days=1):
            td = timedelta(days=1)
        assert check_date <= max_date
        assert check_date >= min_date
        is_ok = check_func(check_date)
        if is_ok:
            last_good = check_date
            check_date -= td
        else:
            last_bad = check_date
            check_date += td
            found_bad = True

        if found_bad and td >= timedelta(days=2):
            td /= 2

    return last_good
