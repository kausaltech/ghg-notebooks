import os
import collections
from datetime import timedelta

import pytz
import luigi
import sqlalchemy as sa
import pandas as pd
from sqlalchemy.dialects.postgresql import insert


class TimescaleDBTarget(luigi.Target):
    _engine_dict = {}  # dict of sqlalchemy engine instances
    Connection = collections.namedtuple("Connection", "engine pid")

    # Override these in a subclass
    measurement_name = None
    value_columns = None
    interval = None  # in seconds
    location_column = None  # optional

    def __init__(self, connection_dsn, start_time, end_time, locations=None):
        self.connection_dsn = connection_dsn
        self.start_time = start_time.astimezone(pytz.utc)
        self.end_time = end_time.astimezone(pytz.utc)
        self.locations = locations

    def _validate(self):
        assert self.measurement_name and isinstance(self.measurement_name, str)
        assert self.location_column is None or isinstance(self.location_column, str)
        assert isinstance(self.interval, int) and self.interval > 0

    @property
    def engine(self):
        """
        Return an engine instance, creating it if it doesn't exist.

        Recreate the engine connection if it wasn't originally created
        by the current process.
        """
        pid = os.getpid()
        conn = TimescaleDBTarget._engine_dict.get(self.connection_dsn)
        if not conn or conn.pid != pid:
            # create and reset connection
            self._validate()
            engine = sa.create_engine(self.connection_dsn, use_batch_mode=True)
            TimescaleDBTarget._engine_dict[self.connection_dsn] = self.Connection(engine, pid)
        return TimescaleDBTarget._engine_dict[self.connection_dsn].engine

    def python_type_to_sqlalchemy(self, klass):
        if klass == int:
            return sa.Integer
        elif klass == float:
            return sa.Float
        raise Exception("Invalid type: %s" % klass)

    def get_table(self):
        Column = sa.Column
        table_name = self.measurement_name

        engine = self.engine
        metadata = sa.MetaData(engine)
        cols = [Column(c[0], self.python_type_to_sqlalchemy(c[1]), nullable=False) for c in self.value_columns]
        if self.location_column:
            cols.insert(0, Column(self.location_column, sa.String, nullable=False))
            cols.append(sa.UniqueConstraint('time', self.location_column))
            time_col = Column('time', sa.TIMESTAMP(timezone=True), nullable=False)
        else:
            time_col = Column('time', sa.TIMESTAMP(timezone=True), nullable=False, unique=True)

        table = sa.Table(table_name, metadata, time_col, *cols)

        with engine.begin() as con:
            if not engine.dialect.has_table(con, table_name):
                table.create(self.engine)
                if self.location_column:
                    ht_args = "'%s', 'time', '%s', 2" % (table_name, self.location_column)
                else:
                    ht_args = "'%s', 'time'" % table_name
                con.execute("SELECT create_hypertable(%s)" % ht_args).fetchall()
                if self.location_column:
                    con.execute("CREATE INDEX ON %s (%s, time DESC)" % (table_name, self.location_column))
                con.execute("SELECT set_chunk_time_interval('%s', interval '1 month')" % table_name).fetchall()

        return table

    def exists(self):
        table = self.get_table()

        conditions = [table.c.time >= self.start_time, table.c.time <= self.end_time]
        if self.locations:
            conditions.append(getattr(table.c, self.location_column).in_(self.locations))
            nr_locations = len(self.locations)
        else:
            nr_locations = 1
        sql = sa.select([sa.func.count(table.c.time)])\
            .where(sa.and_(*conditions))

        with self.engine.connect() as con:
            row_count = con.execute(sql).fetchone()[0]

        time_slots = int((self.end_time - self.start_time) / timedelta(seconds=self.interval))
        time_slots *= nr_locations
        # Allow 1% of the samples to go missing (due to misc. weirdness)
        if row_count < int(0.99 * time_slots):
            return False
        return True

    def get_latest_row(self, before=None):
        table = self.get_table()

        query = table.select()
        if before:
            query = query.where(table.c.time < before)
        res = query.order_by(sa.desc(table.c.time)).limit(1).execute()
        return dict(res.fetchone())

    def write(self, df):
        df = df.copy()
        df.index = df.index.tz_convert('UTC')
        df.index.name = 'time'

        value_column_set = {x[0] for x in self.value_columns}
        location_column_set = set()
        if self.location_column:
            location_column_set.add(self.location_column)
        assert set(df.columns) == value_column_set | location_column_set

        rows = df.reset_index().to_dict('records')

        table = self.get_table()
        stmt = insert(table)
        index_elements = ['time']
        if self.location_column:
            index_elements.append(self.location_column)
        update_set = {col_name: getattr(stmt.excluded, col_name) for col_name in value_column_set}
        # Do an UPSERT: If existing rows conflict, replace their values with
        # incoming data.
        stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_=update_set,
        )
        with self.engine.begin() as con:
            con.execute(stmt, rows)

    def read(self, before=None, after=None):
        table = self.get_table()
        query = table.select()
        if before:
            query = query.where(table.c.time <= before)
        if after:
            query = query.where(table.c.time >= after)
        query = query.order_by(sa.desc(table.c.time))
        with self.engine.connect() as con:
            df = pd.read_sql(query, con, index_col='time')
        df.index = pd.to_datetime(df.index, utc=True)

        return df
