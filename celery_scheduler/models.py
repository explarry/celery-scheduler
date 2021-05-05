from datetime import datetime
from sqlalchemy import Column, Integer, String, JSON, DateTime
from sqlalchemy.orm import declarative_base


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

