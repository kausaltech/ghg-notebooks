from datetime import timedelta, date


MIN_DATE = date(1950, 1, 1)


def find_data_start(check_func, start_date=None):
    """Bisect date ranges to find the first date with valid data"""
    td = timedelta(days=1 << 12)
    found_bad = False
    last_good = None
    last_bad = None

    if not start_date:
        start_date = date.today()

    check_date = start_date - td
    last_bad = check_date
    last_good = start_date

    while (last_good - last_bad) != timedelta(days=1):
        if td < timedelta(days=1):
            td = timedelta(days=1)
        assert check_date >= MIN_DATE
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
