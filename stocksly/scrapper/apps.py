from django.apps import AppConfig
from django.db.models.signals import post_migrate

class ScrapperConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'scrapper'

    def ready(self):
        from .signals import schedule_tasks
        post_migrate.connect(schedule_tasks, sender=self)
