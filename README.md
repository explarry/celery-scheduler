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

