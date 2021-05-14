# celery-scheduler

## Introduction

`celery-scheduler` provides schedulers for celery beat to enable dynamic beat task config.

## Install

pass

## Usage

1. run celery beat with `--scheduler`, for example,

```shell
celery -A tasks beat --scheduler celery_scheduler.ShelveScheduler
```

2. dynamic config in python shell, for example,

```python
from celery.schedules import crontab
from celery_scheduler import ShelveChanges

# add a beat task entry
ShelveChanges().add_task({'name': 'custom_backend_cleanup', 'task': 'celery.backend_cleanup', 'schedule': 15})

# update a beat task entry
ShelveChanges().update_task({'name': 'custom_backend_cleanup', 'task': 'celery.backend_cleanup', 'schedule': crontab(hour=7, minute=30)})

# delete a beat task entry
ShelveChanges().delete_task('custom_backend_cleanup')
```

3. available scheduler and changes

* `celery_scheduler.FileScheduler` and `celery_scheduler.FileChanges`

* `celery_scheduler.ShelveScheduler` and `celery_scheduler.ShelveChanges`

* `celery_scheduler.DatabaseScheduler` and `celery_scheduler.DatabaseChanges`

  1. default database uri is `sqlite:////[path to celery_scheduler module]/celerybeat-schedule`

  2. for `DatabaseScheduler`, specify database_uri before starting celery beat, for example,

    `app.conf.update(database_url=your_database_uri)`

  3. for `DatabaseChanges`, specify database_uri when managing beat task entries, for example,

    `DatabaseChanges(your_database_uri)` or `DatabaseChanges(database_uri=your_database_uri)`

  4. your database uri have to be supported by sqlalchemy, for example,

    `your_database_uri = sqlite:///celerybeat-schedule`,

    `your_database_uri = postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/celery-schedule`,

    `your_database_uri = mysql+mysqlconnector://root:root@127.0.0.1:3306/celery-schedule`,

    and etc.
 
