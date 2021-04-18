import os
import shelve
import time


from celery.beat import PersistentScheduler, ScheduleEntry
from celery.utils.log import get_logger


logger = get_logger(__name__)
current_dir = os.path.dirname(os.path.abspath(__file__))
changes_file_path = os.path.join(current_dir, 'celerybeat-changes')


class Changes(object):
    _retry = 10
    _interval = 0.01
    _changes_file_path = os.path.join(current_dir, 'celerybeat-changes')

    def _open(self):
        self._changes = shelve.open(self._changes_file_path, writeback=True)

    def _close(self):
        if hasattr(self, '_changes'):
            self._changes.close()

    def _try_open(self):
        for _ in range(self._retry):
            try:
                self._close()
                self._open()
            except IOError as exc:
                logger.warning('try open failed<==%s', exc)
                time.sleep(self._interval)
                continue
            else:
                return True
        logger.error('try open failed for 10 consecutive times, stop trying')
        return False

    @staticmethod
    def _get_task_name(task):
        task_name = task.get('name') or task['task']
        if not isinstance(task['task'], str):
            raise KeyError('value of key task must be string')
        return task_name

    def add_task(self, task):
        self._open()
        self._changes.setdefault('operations', [])
        task_name = self._get_task_name(task)
        self._changes['operations'].append(('add', task_name, task))
        self._close()
        logger.info(f'add task, task={task}')

    def delete_task(self, task_name):
        self._open()
        self._changes.setdefault('operations', [])
        self._changes['operations'].append(('delete', task_name, None))
        self._close()
        logger.info(f'delete task, task_name={task_name}')

    def update_task(self, task):
        self.add_task(task)

    def get_and_clear_operations(self):
        if not self._try_open():
            return []
        operations = self._changes.get('operations', [])
        self._changes.clear()
        self._close()
        return operations


class FileScheduler(PersistentScheduler):
    sync_every = 10
    changes_class = Changes

    def __init__(self, *args, **kwargs):
        self.changes = self.changes_class()
        PersistentScheduler.__init__(self, *args, **kwargs)

    def sync(self):
        if self._store is None:
            return
        for operation, task_name, task in self.changes.get_and_clear_operations():
            logger.debug(f'operation={operation}, task_name={task_name}, task={task}')
            if operation == 'add':
                self._store['entries'].update({
                    task_name: self._maybe_entry(task_name, task)
                })
            elif operation == 'delete':
                del self._store['entries'][task_name]
        self._store.sync()

