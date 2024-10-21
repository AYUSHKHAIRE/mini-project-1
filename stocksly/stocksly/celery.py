# stocksly/celery.py

import os
from celery import Celery
from celery.schedules import crontab
from django.conf import settings

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'stocksly.settings')

app = Celery('stocksly')

# Load configuration from Django settings
app.config_from_object('django.conf:settings', namespace='CELERY')

# Autodiscover tasks from all registered Django apps
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')

# Celery Beat schedule for daily tasks
app.conf.beat_schedule = {
    'daily-update-task': {
        'task': 'scrapper.views.update_data_for_today',  # Path to task in scrapper app
        'schedule': crontab(hour=8, minute=00),   # Runs daily at 07:20 UTC
    },
}

app.conf.timezone = 'UTC'
