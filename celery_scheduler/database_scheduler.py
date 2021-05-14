import os
import numbers
import datetime


from celery import schedules
from celery.beat import Scheduler
from celery.utils.log import get_logger
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from typing import Any, Dict


from .models import Base, TaskEntry


logger = get_logger(__name__)
current_dir = os.path.dirname(os.path.abspath(__file__))


def _serialize_schedule(schedule: Any):
    if isinstance(schedule, schedules.schedule):
        schedule = schedule.run_every

    if isinstance(schedule, numbers.Number):
        return schedule
    elif isinstance(schedule, datetime.timedelta):
        return schedule.total_seconds()
    elif isinstance(schedule, schedules.crontab):
        return {
            'minute': schedule._orig_minute,
            'hour': schedule._orig_hour,
            'day_of_week': schedule._orig_day_of_week,
            'day_of_month': schedule._orig_day_of_month,
            'month_of_year': schedule._orig_month_of_year,
        }
        return schedule.total_seconds()
    elif isinstance(schedule, schedules.solar):
        return {
            'event': schedule.event,
            'latitude': schedule.lat,
            'longtitude': schedule.lon,
        }
    raise TypeError('serialize schedule failed<==unsupproted schedule, schedule=%s' % schedule)


def _deserialize_schedule(schedule: Any):
    if isinstance(schedule, numbers.Number):
        return schedule
    elif isinstance(schedule, dict):
        if 'event' in schedule:
            return schedules.solar(
               schedule.get('event'),
               schedule.get('latitude'),
               schedule.get('longtitude')
            )
        return schedules.crontab(
            minute=schedule.get('minute', '*'),
            hour=schedule.get('hour', '*'),
            day_of_week=schedule.get('day_of_week', '*'),
            day_of_month=schedule.get('day_of_month', '*'),
            month_of_year=schedule.get('month_of_year', '*')
        )
    raise TypeError('deserialize schedule failed<==unsupproted schedule, schedule=%s' % schedule)


class DatabaseChanges(object):
    _database_uri = 'sqlite:///%s' % os.path.join(current_dir, 'celerybeat-schedule')

    def __init__(self, database_uri: str = None):
        self._database_uri = database_uri or self._database_uri
        self.engine = create_engine(self._database_uri)
        self.Session = sessionmaker(self.engine)
        self.session = self.Session()

    def _create_table(self):
        Base.metadata.create_all(self.engine)
        logger.info('create table succeeded')

    @staticmethod
    def _get_task_name(task: Dict) -> str:
        task_name = task.get('name') or task['task']
        if not isinstance(task['task'], str):
            raise KeyError('value of key task must be string')
        return task_name

    def add_task(self, task: Dict):
        task_name = self._get_task_name(task)
        row = self.session.query(TaskEntry).filter_by(name=task_name).first() or TaskEntry()
        row.name = task_name
        row.task = task['task']
        row.args = task.get('args', [])
        row.kwargs = task.get('kwargs', {})
        row.options = task.get('options', {})
        row.schedule = _serialize_schedule(task['schedule'])
        self.session.merge(row)
        self.session.commit()
        logger.info(f'add task, task={task}')

    def delete_task(self, task_name: str):
        self.session.query(TaskEntry).filter_by(name=task_name).delete()
        self.session.commit()
        logger.info(f'delete task, task_name={task_name}')

    def update_task(self, task: Dict):
        self.add_task(task)


class DatabaseScheduler(Scheduler):
    changes_class = DatabaseChanges
    max_interval = 10
    sync_every = 10

    def __init__(self, *args, **kwargs):
        if len(args) == 0:
            app = kwargs['app']
        else:
            assert len(args) == 1
            app = args[0]
        self.changes = self.changes_class(app.conf.get('database_uri'))
        self.session = self.changes.session
        Scheduler.__init__(self, *args, **kwargs)

    def _read_schedule_from_table(self) -> Dict:
        rows = self.session.query(TaskEntry).all()
        schedule = {}
        for row in rows:
            schedule[row.name] = {
                'name': row.name,
                'task': row.task,
                'args': row.args,
                'kwargs': row.kwargs,
                'options': row.options,
                'schedule': _deserialize_schedule(row.schedule),
            }
        logger.debug('schedule=%s', schedule)
        return schedule

    def _write_schedule_to_table(self):
        for name, entry in self.schedule.items():
            logger.debug('task=%s, schedule=%s', entry.task, entry.schedule)
            row = self.session.query(TaskEntry).filter_by(name=name).first() or TaskEntry()
            row.name = name
            row.task = entry.task
            row.args = entry.args
            row.kwargs = entry.kwargs
            row.options = entry.options
            row.schedule = _serialize_schedule(entry.schedule)
            row.last_run_at = entry.last_run_at
            row.total_run_count = entry.total_run_count
            self.session.merge(row)

    def setup_schedule(self):
        self.changes._create_table()
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.beat_schedule)
        self.update_from_dict(self._read_schedule_from_table())
        self._write_schedule_to_table()

    def sync(self):
        logger.debug('sync started')
        try:
            persistent_data = self._read_schedule_from_table()
            self.merge_inplace(persistent_data)
            self._write_schedule_to_table()
        except BaseException as exc:
            self.session.rollback()
            logger.warning('sync failed<==%s', exc)
        else:
            self.session.commit()
        logger.debug('sync finished')

    def close(self):
        self.session.close()

    @property
    def info(self):
        return '    . db -> %s' % self.changes._database_uri

