import os
import json
import logging
from datetime import datetime

import luigi
import pandas as pd
from tzlocal import get_localzone

from data_import import digitraffic
import settings

from .targets.timescaledb import TimescaleDBTarget


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_selected_stations():
    task = DigitrafficTMSStations()
    if not task.complete():
        task.run()

    with task.output().open('r') as in_file:
        stations = json.load(in_file)
    return stations


def get_station_by_name(station_name):
    stations = get_selected_stations()

    filtered = list(filter(lambda x: x['name'] == station_name, stations))
    if len(filtered) != 1:
        station_names = ', '.join([x['name'] for x in stations])
        raise Exception('Invalid station name: %s\nChoices: %s' % (station_name, station_names))
    return filtered[0]


class DigitrafficTarget(TimescaleDBTarget):
    measurement_name = 'digitraffic_vehicle_count'
    value_columns = [(x, int) for x in digitraffic.VEHICLE_CLASSES.values()]
    location_column = 'station_direction'
    interval = 60


class DigitrafficVehicleCountRaw(luigi.Task):
    station_name = luigi.Parameter()
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.station = get_station_by_name(self.station_name)

    def output(self):
        date = self.date
        path = 'digitraffic/lam/%d/%d/%d/%s.csv' % (date.year, date.month, date.day, self.station_name)
        return luigi.LocalTarget(os.path.join(settings.DATA_DIR, path))

    def run(self):
        content = digitraffic.fetch_tms_station_raw_data('01', self.station['id'], self.date)
        with self.output().open('w') as out:
            out.write(content)

    @classmethod
    def bulk_complete(cls, parameter_tuples):
        completed = []
        for date in parameter_tuples:
            task = cls(date=date)
            if task.complete():
                completed.append(date)
        return completed


class DigitrafficTMSStations(luigi.Task):
    def output(self):
        path = 'digitraffic/lam_stations.json'
        return luigi.LocalTarget(os.path.join(settings.DATA_DIR, path))

    def run(self):
        stations = digitraffic.get_tms_stations()
        muni_id = int(settings.MUNICIPALITY_ID)
        muni_stations = list(filter(lambda x: x['municipality_code'] and int(x['municipality_code']) == muni_id, stations))
        with self.output().open('w') as out_file:
            json.dump(muni_stations, out_file, indent=4)


class DigitrafficVehicleCountDaily(luigi.Task):
    station_name = luigi.Parameter()
    date = luigi.DateParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        start_time = datetime.combine(self.date, datetime.min.time())
        end_time = datetime.combine(self.date, datetime.max.time())
        local_tz = get_localzone()
        self.start_time = local_tz.localize(start_time)
        self.end_time = local_tz.localize(end_time)

        self.station = get_station_by_name(self.station_name)

    def requires(self):
        return [DigitrafficVehicleCountRaw(self.station_name, self.date)]

    def _get_location_names(self):
        station = self.station
        directions = [station['direction%d_municipality' % i] for i in (1, 2)]
        locations = ['%s_(%s)' % (station['name'], d) for d in directions]
        return locations

    def output(self):
        return DigitrafficTarget(
            settings.POSTGRESQL_DSN, start_time=self.start_time, end_time=self.end_time,
            locations=self._get_location_names()
        )

    def run(self):
        with self.input()[0].open('r') as f:
            content = f.read()
            df = digitraffic.parse_tms_station_raw_data(self.station['id'], self.date, content)

        # Group in buckets of one minute, columns as counts by vehicle type
        df = df.groupby([pd.Grouper(freq='60s', sort=True), 'direction', 'vehicle_class']).size()\
            .unstack('vehicle_class', fill_value=0)

        # Fill in gaps in time index
        dir_groups = df.groupby('direction')
        dir_dfs = []
        for dir_name, dir_df in dir_groups:
            dir_df = dir_df.reset_index('direction').drop(columns='direction').resample('1min').asfreq(fill_value=0)
            dir_df['direction'] = dir_name
            dir_dfs.append(dir_df)

        df = pd.concat(dir_dfs).reset_index().set_index(['time', 'direction'])

        # Set directions (1, 2) to more descriptive names
        assert list(df.index.levels[1]) == [1, 2]
        df.index.set_levels(levels=self._get_location_names(), level='direction', inplace=True)

        df = df.reset_index('direction').rename(columns=dict(direction='station_direction'))

        target = self.output()
        target.write(df)

    @classmethod
    def bulk_complete(cls, parameter_tuples):
        completed = []
        for date in parameter_tuples:
            task = cls(date=date)
            if task.complete():
                completed.append(date)
        return completed
