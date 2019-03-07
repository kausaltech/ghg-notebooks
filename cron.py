import luigi
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor

import tasks.fingrid
from data_import.fingrid import MEASUREMENTS


executors = {
    'default': ProcessPoolExecutor(1)
}
sched = BlockingScheduler(executors=executors)


@sched.scheduled_job('interval', hours=1)
def update_fingrid_measurements():
    tasks_to_run = [tasks.fingrid.FingridLast24hTask(measurement_name=m) for m in MEASUREMENTS.keys()]
    luigi.build(tasks_to_run)


sched.start()
