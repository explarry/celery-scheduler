from datetime import datetime
from sqlalchemy.event import listen
from sqlalchemy import Column, Integer, String, JSON, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import select as _select, insert as _insert, update as _update, delete as _delete


Base = declarative_base()


class TaskEntry(Base):
    __tablename__ = 'task_entry'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    task = Column(String, nullable=False)
    args = Column(JSON, default=[])
    kwargs = Column(JSON, default={})
    options = Column(JSON, default={})
    schedule = Column(JSON, default={}, nullable=False)
    last_run_at = Column(DateTime, default=datetime.now)
    total_run_count = Column(Integer)


class TaskEntryChange(Base):
    """
    operation is limited to 'add', 'delete', 'update'
    """

    __tablename__ = 'task_entry_change'

    id = Column(Integer, primary_key=True)
    operation = Column(String, nullable=False)
    name = Column(String, unique=True, nullable=False)
    task = Column(String)
    args = Column(JSON, default=[])
    kwargs = Column(JSON, default={})
    options = Column(JSON, default={})
    schedule = Column(JSON, default={})


def add(mapper, connection, target):
    _ = mapper
    s = connection.execute(
        _select([TaskEntryChange]).where(TaskEntryChange.name == target.name).limit(1)
    ).one_or_none()
    if not s:
        connection.execute(
            _insert(TaskEntryChange),
            name=target.name,
            operation='add',
            task=target.task,
            args=target.args,
            kwargs=target.kwargs,
            options=target.options,
            schedule=target.schedule,
        )
    else:
        connection.execute(
            _update(TaskEntryChange).where(TaskEntryChange.name == target.name).values(
                operation='add',
                task=target.task,
                args=target.args,
                kwargs=target.kwargs,
                options=target.options,
                schedule=target.schedule,
            )
        )


def delete(mapper, connection, target):
    _ = mapper
    connection.execute(
        _delete(TaskEntryChange).where(TaskEntryChange.name == target.name)
    )


def update(mapper, connection, target):
    add(mapper, connection, target)


listen(TaskEntry, 'after_insert', add)
listen(TaskEntry, 'after_delete', delete)
listen(TaskEntry, 'after_update', update)

