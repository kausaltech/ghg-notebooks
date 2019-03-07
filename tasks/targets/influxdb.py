import luigi
import pytz
from influxdb import DataFrameClient


class InfluxDBTarget(luigi.Target):
    def __init__(self, connection_dsn, measurement_name, start_time, end_time, interval):
        self.client = DataFrameClient.from_dsn(connection_dsn)
        self.measurement_name = measurement_name
        self.start_time = start_time.astimezone(pytz.utc)
        self.end_time = end_time.astimezone(pytz.utc)
        self.interval = interval

    def exists(self):
        query_str = 'SELECT COUNT(*) FROM "%s" WHERE time >= \'%s\' AND time <= \'%s\' GROUP BY time(%ds)' % \
            (self.measurement_name, self.start_time.isoformat(), self.end_time.isoformat(), self.interval)
        results = self.client.query(query_str)
        if self.measurement_name not in results:
            return False
        df = results[self.measurement_name]
        column = df[df.columns[0]]
        missing = len(df[column != 1])
        # Allow 1% missing values to compensate for occasinal weirdness
        if missing > int(len(column) * 0.9):
            return False
        else:
            return True

    def write(self, df):
        df = df.copy()
        df.index = df.index.tz_convert('UTC')
        self.client.write_points(df, self.measurement_name, time_precision='s')
