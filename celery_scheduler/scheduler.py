import fcntl
import pickle
import os
import shelve
import time


from celery.beat import PersistentScheduler, ScheduleEntry
from celery.utils.log import get_logger
from typing import List, Dict


logger = get_logger(__name__)
current_dir = os.path.dirname(os.path.abspath(__file__))
changes_file_path = os.path.join(current_dir, 'celerybeat-changes')


class Changes(object):
    _retry = 10
    _interval = 0.01
    _changes_file_path = os.path.join(current_dir, 'celerybeat-changes')
    
    @staticmethod
    def _get_task_name(task: Dict) -> str:
        task_name = task.get('name') or task['task']
        if not isinstance(task['task'], str):
            raise KeyError('value of key task must be string')
        return task_name

    def _open(self):
        raise NotImplemented

    def _close(self):
        raise NotImplemented

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
        logger.error('try open failed for %s consecutive times, stop trying', self._retry)
        return False

    def add_task(self, task: Dict):
        raise NotImplemented

    def delete_task(self, task_name: str):
        raise NotImplemented

    def update_task(self, task: Dict):
        self.add_task(task)

    def get_and_clear_operations(self) -> List:
        raise NotImplemented


class FileChanges(Changes):
    def _open(self):
        self._changes = open(self._changes_file_path, 'ab+')
        fcntl.flock(self._changes, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _close(self):
        if hasattr(self, '_changes'):
            fcntl.flock(self._changes.fileno(), fcntl.LOCK_UN)
            self._changes.close()

    def add_task(self, task: Dict):
        self._open()
        task_name = self._get_task_name(task)
        self._changes.write(b'%s,%s,%s\n' % (b'add', task_name.encode(), pickle.dumps(task)))
        self._close()
        logger.info(f'add task, task={task}')

    def delete_task(self, task_name: str):
        self._open()
        self._changes.write(b'%s,%s,%s\n' % (b'delete', task_name.encode(), b'null'))
        self._close()
        logger.info(f'delete task, task_name={task_name}')

    def get_and_clear_operations(self) -> List:
        self._open()
        self._changes.seek(0)
        operations = []
        for line in self._changes:
            operation = line.strip().split(b',')
            if operation[0] == b'add':
                operation[0] = 'add'
                operation[1] = operation[1].decode()
                operation[2] = pickle.loads(operation[2])
            elif operation[0] == b'delete':
                operation[0] = 'delete'
                operation[1] = operation[1].decode()
                operation[2] = None
            else:
                continue
            operations.append(operation)
        self._changes.seek(0)
        self._changes.truncate()
        self._close()
        return operations


class ShelveChanges(Changes):
    def _open(self):
        self._changes = shelve.open(self._changes_file_path, writeback=True)

    def _close(self):
        if hasattr(self, '_changes'):
            self._changes.close()

    def add_task(self, task: Dict):
        self._open()
        self._changes.setdefault('operations', [])
        task_name = self._get_task_name(task)
        self._changes['operations'].append(('add', task_name, task))
        self._close()
        logger.info(f'add task, task={task}')

    def delete_task(self, task_name: str):
        self._open()
        self._changes.setdefault('operations', [])
        self._changes['operations'].append(('delete', task_name, None))
        self._close()
        logger.info(f'delete task, task_name={task_name}')

    def get_and_clear_operations(self) -> List:
        if not self._try_open():
            return []
        operations = self._changes.get('operations', [])
        self._changes.clear()
        self._close()
        return operations


class Scheduler(PersistentScheduler):
    max_interval = 10
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


class FileScheduler(Scheduler):
    changes_class = FileChanges


class ShelveScheduler(Scheduler):
    changes_class = ShelveChanges

