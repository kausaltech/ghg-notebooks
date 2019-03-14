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


class FingridDBTarget(TimescaleDBTarget):
    pass


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
        target = FingridDBTarget(settings.POSTGRESQL_DSN, start_time=self.start_time, end_time=self.end_time)
        target.measurement_name = 'fingrid_%s' % self.measurement_name
        target.value_columns = [(self.meta_data['quantity'], float)]
        target.interval = self.meta_data['interval']
        return target

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


class FingridLast24hTask(FingridTask, luigi.Task):
    measurement_name = luigi.ChoiceParameter(choices=fingrid.MEASUREMENTS.keys())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        self.fingrid_init(start_time, end_time)


class FingridLast24hUpdateAllTask(luigi.Task):
    def complete(self):
        return False

    def run(self):
        for measurement_name in fingrid.MEASUREMENTS.keys():
            yield FingridLast24hTask(measurement_name=measurement_name)


class FingridMonthlyAllMeasurementsTask(luigi.Task):
    month = luigi.MonthParameter()

    def requires(self):
        measurements = fingrid.MEASUREMENTS.items()
        tasks = []
        for name, m in measurements:
            earliest_date = m.get('start_date')
            if earliest_date and self.month < earliest_date:
                continue
            tasks.append(FingridMonthlyTask(month=self.month, measurement_name=name))
        return tasks

    def complete(self):
        return all([task.complete() for task in self.requires()])

    def run(self):
        pass
