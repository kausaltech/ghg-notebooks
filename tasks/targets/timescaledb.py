import os
import collections
from datetime import timedelta

import pytz
import luigi
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert


class TimescaleDBTarget(luigi.Target):
    _engine_dict = {}  # dict of sqlalchemy engine instances
    Connection = collections.namedtuple("Connection", "engine pid")

    def __init__(self, connection_dsn, measurement_name, value_column_name, start_time, end_time, interval):
        self.connection_dsn = connection_dsn
        self.measurement_name = measurement_name
        self.start_time = start_time.astimezone(pytz.utc)
        self.end_time = end_time.astimezone(pytz.utc)
        self.value_column_name = value_column_name
        self.interval = interval

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
            engine = sqlalchemy.create_engine(self.connection_dsn, use_batch_mode=True, echo=False)
            TimescaleDBTarget._engine_dict[self.connection_dsn] = self.Connection(engine, pid)
        return TimescaleDBTarget._engine_dict[self.connection_dsn].engine

    def get_table(self):
        Column = sqlalchemy.Column
        table_name = self.measurement_name

        engine = self.engine
        metadata = sqlalchemy.MetaData(engine)
        table = sqlalchemy.Table(
            table_name, metadata,
            Column('time', sqlalchemy.TIMESTAMP(timezone=True), nullable=False, unique=True),
            Column(self.value_column_name, sqlalchemy.Float, nullable=False),
        )
        with engine.begin() as con:
            if not engine.dialect.has_table(con, table_name):
                table.create(self.engine)
                con.execute("SELECT create_hypertable('%s', 'time')" % table_name).fetchall()
                con.execute("SELECT set_chunk_time_interval('%s', interval '1 month')" % table_name).fetchall()

        return table

    def exists(self):
        table = self.get_table()
        sql = sqlalchemy.select([sqlalchemy.func.count(table.c.time)])\
            .where(sqlalchemy.and_(
                table.c.time >= self.start_time, table.c.time <= self.end_time
            ))
        with self.engine.connect() as con:
            row_count = con.execute(sql).fetchone()[0]
        time_slots = int((self.end_time - self.start_time) / timedelta(seconds=self.interval))
        # Allow 1% of the samples to go missing (due to misc. weirdness)
        if row_count < int(0.99 * time_slots):
            return False
        return True

    def write(self, df):
        df = df.copy()
        df.index = df.index.tz_convert('UTC')
        df.index.name = 'time'
        assert len(df.columns) == 1
        column_map = {
            df.columns[0]: self.value_column_name,
        }
        rows = df.rename(columns=column_map).reset_index().to_dict('records')

        table = self.get_table()
        stmt = insert(table)
        # Do an UPSERT: If existing rows conflict, replace their values with
        # incoming data.
        stmt = stmt.on_conflict_do_update(
            index_elements=['time'],
            set_={self.value_column_name: getattr(stmt.excluded, self.value_column_name)}
        )
        with self.engine.begin() as con:
            con.execute(stmt, rows)
