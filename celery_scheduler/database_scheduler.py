import os
import numbers
import datetime


from celery.beat import Scheduler
from celery.schedules import crontab, solar
from celery.utils.log import get_logger
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from typing import Any, Dict


from .models import Base, TaskEntry


logger = get_logger(__name__)
current_dir = os.path.dirname(os.path.abspath(__file__))


class DatabaseScheduler(Scheduler):
    _schedule_uri = 'sqlite:///%s' % os.path.join(current_dir, 'celerybeat-schedule.db')
    sync_every = 10

    def __init__(self, *args, **kwargs):
        self.engine = create_engine(self._schedule_uri)
        self.Session = sessionmaker(self.engine)
        self.session = self.Session()
        Scheduler.__init__(self, *args, **kwargs)

    def _create_table(self):
        Base.metadata.create_all(self.engine)
        logger.info('create table succeeded')

    @staticmethod
    def _deserialize_schedule(schedule: Any):
        if isinstance(schedule, numbers.Number):
            return schedule
        elif isinstance(schedule, dict):
            if 'event' in schedule:
                return solar(
                   schedule.get('event'),
                   schedule.get('latitude'),
                   schedule.get('longtitude')
                )
            return crontab(
                minute=schedule.get('minute', '*'),
                hour=schedule.get('hour', '*'),
                day_of_week=schedule.get('day_of_week', '*'),
                day_of_month=schedule.get('day_of_month', '*'),
                month_of_year=schedule.get('month_of_year', '*')
            )
        raise TypeError('deserialize schedule failed<==unsupproted schedule, schedule=%s' % schedule)

    @staticmethod
    def _serialize_schedule(schedule: Any):
        if isinstance(schedule, numbers.Number):
            return schedule
        elif isinstance(schedule, datetime.timedelta):
            return schedule.total_seconds()
        elif isinstance(schedule, crontab):
            return {
                'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year,
            }
            return schedule.total_seconds()
        elif isinstance(schedule, solar):
            return {
                'event': schedule.event,
                'latitude': schedule.lat,
                'longtitude': schedule.lon,
            }
        raise TypeError('serialize schedule failed<==unsupproted schedule, schedule=%s' % schedule)

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
                'schedule': self._deserialize_schedule(row.schedule),
            }
        return schedule

    def _write_schedule_to_table(self):
        logger.info('self.schedule=%s', self.schedule)
        for name, entry in self.schedule.items():
            logger.info('entry.schedule=%s', entry.schedule)
            row = self.session.query(TaskEntry).filter_by(task=name).first() or TaskEntry()
            row.name = name
            row.task = entry.task
            row.args = entry.args
            row.kwargs = entry.kwargs
            row.options = entry.options
            row.schedule = self._serialize_schedule(entry.schedule)
            row.last_run_at = entry.last_run_at
            row.total_run_count = entry.total_run_count
            self.session.merge(row)
            # cnt = self.session.query(TaskEntry).filter_by(task=entry.name).count()
            # if cnt == 0:
            #     self.session.add(TaskEntry(
            #         name=entry.name,
            #         task=entry.task,
            #         args=entry.args,
            #         kwargs=entry.args,
            #         options=entry.options,
            #         schedule=self._serialize_schedule(self.schedule),
            #         last_run_at = entry.last_run_at,
            #         total_run_count = entry.total_run_count,
            #     ))
            # else:
            #     self.session.query(TaskEntry).filter_by(task=entry.name).update(dict(
            #         task=entry.task,
            #         args=entry.args,
            #         kwargs=entry.args,
            #         options=entry.options,
            #         schedule=self._serialize_schedule(self.schedule),
            #         last_run_at = entry.last_run_at,
            #         total_run_count = entry.total_run_count,
            #     ))

    def setup_schedule(self):
        self._create_table()
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.beat_schedule)
        self.update_from_dict(self._read_schedule_from_table())
        self._write_schedule_to_table()

    def sync(self):
        logger.info('sync started')
        try:
            persistent_data = self._read_schedule_from_table()
            self.merge_inplace(persistent_data)
            self._write_schedule_to_table()
        except BaseException as exc:
            self.session.rollback()
            logger.info('sync failed<==%s', exc)
        else:
            self.session.commit()
        logger.info('sync finished')

    def close(self):
        self.session.close()

    @property
    def info(self):
        return '    . db -> %s' % self._schedule_uri

