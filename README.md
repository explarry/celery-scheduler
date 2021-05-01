# celery-scheduler

## Introduction

`celery-scheduler` provides schedulers for celery beat to enable dynamic beat task config.

## Install

pass

## Usage

1. run celery beat with `--scheduler` and `--max-interval` options, for example,

```shell
celery -A tasks beat --scheduler celery_scheduler.scheduler.ShelveScheduler --max-interval 10
```

2. dynamic config in python shell, for example,

```python
from celery.schedules import crontab
from celery_scheduler.scheduler import ShelveChanges

# add a beat task entry
ShelveChanges().add_task({'name': 'custom_backend_cleanup', 'task': 'celery.backend_cleanup', 'schedule': 15})

# update a beat task entry
ShelveChanges().update_task({'name': 'custom_backend_cleanup', 'task': 'celery.backend_cleanup', 'schedule': crontab(hour=7, minute=30)})

# delete a beat task entry
ShelveChanges().delete_task('custom_backend_cleanup')
```

3. available scheduler and changes

* `FileScheduler` and `FileChanges`
* `ShelveScheduler` and `ShelveChanges`

