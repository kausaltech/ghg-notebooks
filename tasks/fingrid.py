import logging
from datetime import timedelta, datetime, date

import luigi
import pandas as pd

from data_import import fingrid
import settings

from .targets.timescaledb import TimescaleDBTarget
from .targets.quilt import QuiltDataframeTarget


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = logging.getLogger('luigi-interface').handlers


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

        local_tz = fingrid.LOCAL_TZ
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

        assert self.month.day == 1
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


class FingridUpdateQuiltTask(luigi.Task):
    measurement_type = luigi.ChoiceParameter(choices=['power', 'temperature'])
    quilt_package_name = 'fingrid_realtime'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        last_day = date.today().replace(day=1) - timedelta(days=1)
        self.last_month = last_day.replace(day=1)
        self.end_time = datetime.combine(last_day, datetime.max.time())

        self.measurements = [(n, m) for n, m in fingrid.MEASUREMENTS.items() if self.include_measurement(m)]

    def include_measurement(self, m):
        return m['interval'] == fingrid.THREE_MIN and m['quantity'] == self.measurement_type

    def requires(self):
        return [FingridMonthlyTask(month=self.last_month, measurement_name=name) for name, m in self.measurements]

    def output(self):
        targets = self.requires()
        latest_rows = (t.output().get_latest_row(before=self.end_time) for t in targets)
        latest_ts = max((r['time'] for r in latest_rows))
        target = QuiltDataframeTarget(self.quilt_package_name, self.measurement_type, timestamp=latest_ts)
        return target

    def complete(self):
        return False

    def process_df(self, df):
        return df

    def run(self):
        target = self.output()
        frames = []
        for (measurement_name, m), task in zip(self.measurements, self.requires()):
            self.set_status_message("Reading %s" % task.measurement_name)
            logger.info('Reading %s' % task.measurement_name)
            df = task.output().read(before=self.end_time)
            df = self.process_df(df)
            assert len(df.columns) == 1
            logger.info('Read %d rows' % len(df))
            col_name = df.columns[0]
            assert col_name == m['quantity']
            df[col_name] = df[col_name].astype('pint[%s]' % m['unit'])
            name = task.measurement_name.replace('_3m', '').replace('_hourly', '')
            df.rename(columns={col_name: name}, inplace=True)
            frames.append(df)

        df = frames[0]
        out = df.join(frames[1:])
        out.index = pd.to_datetime(df.index, utc=True).tz_convert('Europe/Helsinki')

        target.update(out)
        target.push()


class FingridUpdateQuiltHourlyTask(FingridUpdateQuiltTask):
    quilt_package_name = 'fingrid_hourly'

    def process_df(self, df):
        return df.groupby(pd.Grouper(freq='1h')).mean()
