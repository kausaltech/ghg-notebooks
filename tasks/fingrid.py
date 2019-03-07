import logging
import luigi
from datetime import timedelta, datetime, date
from tzlocal import get_localzone

from data_import import fingrid
import settings

from .targets.timescaledb import TimescaleDBTarget


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


if not settings.FINGRID_API_KEY:
    raise Exception("FINGRID_API_KEY not specified in settings")

if not settings.QUILT_USER:
    raise Exception("QUILT_USER not specified in settings")


class FingridTask:
    def fingrid_init(self, start_time, end_time):
        self.meta_data = fingrid.get_measurement_meta_data(self.measurement_name)

        earliest_time = datetime.combine(self.meta_data['start_date'], datetime.min.time())
        if start_time < earliest_time:
            raise Exception("Date interval before data start date (%s)" % self.meta_data['start_date'])

        local_tz = get_localzone()
        self.start_time = local_tz.localize(start_time)
        self.end_time = local_tz.localize(end_time)
        self.end_time -= timedelta(seconds=1)

    def output(self):
        return TimescaleDBTarget(
            settings.POSTGRESQL_DSN, measurement_name='fingrid_%s' % self.measurement_name,
            value_column_name=self.meta_data['quantity'],
            start_time=self.start_time, end_time=self.end_time,
            interval=self.meta_data['interval']
        )

    def run(self):
        fingrid.set_api_key(settings.FINGRID_API_KEY)
        df = fingrid.get_measurements(self.measurement_name, self.start_time, self.end_time)
        df[df.columns[0]] = df[df.columns[0]].pint.m
        target = self.output()
        target.write(df)


class FingridDateIntervalTask(FingridTask, luigi.Task):
    measurement_name = luigi.ChoiceParameter(choices=fingrid.MEASUREMENTS.keys())
    date_interval = luigi.DateIntervalParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        start_time = datetime.combine(self.date_interval.date_a, datetime.min.time())
        end_time = datetime.combine(self.date_interval.date_b, datetime.min.time())
        self.fingrid_init(start_time, end_time)


class FingridMonthlyTask(FingridTask, luigi.Task):
    measurement_name = luigi.ChoiceParameter(choices=fingrid.MEASUREMENTS.keys())
    month = luigi.MonthParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        start_time = datetime.combine(self.month, datetime.min.time())
        # First of next month
        end_time = (start_time + timedelta(days=32)).replace(day=1)
        self.fingrid_init(start_time, end_time)
