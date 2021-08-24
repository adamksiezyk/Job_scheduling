from datetime import timedelta, datetime
from unittest import TestCase

from src.model.entities.job import ScheduledJob
from src.model.entities.project import Project


class TestScheduledJob(TestCase):
    def setUp(self) -> None:
        self.project = Project(start_dt=datetime(2021, 3, 1, 6), expiration_dt=datetime(2021, 3, 1, 6), id="P1")
        self.scheduled_job_day_delay = ScheduledJob(duration=timedelta(hours=6), machine_id="M1", delay="2d",
                                                    project=self.project, end_dt=datetime(2021, 4, 1, 6),
                                                    start_dt=datetime(2021, 3, 28, 14))
        self.scheduled_job_week_delay = ScheduledJob(duration=timedelta(hours=6), machine_id="M1", delay="1w",
                                                     project=self.project, end_dt=datetime(2021, 4, 1, 6),
                                                     start_dt=datetime(2021, 3, 28, 14))

    def test_parse_delay(self) -> None:
        self.assertEqual(self.scheduled_job_day_delay.parse_delay(), datetime(2021, 4, 3, 6))
        self.assertEqual(self.scheduled_job_week_delay.parse_delay(), datetime(2021, 4, 5, 6))