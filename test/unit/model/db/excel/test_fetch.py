import unittest
from datetime import datetime, timedelta

import pandas as pd

from src.model.db.excel.fetch import fetch_project, fetch_jobs, fetch_all_jobs, fetch_resources, fetch_all_resources
from src.model.entities.job import Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource


class TestFetch(unittest.TestCase):
    def setUp(self):
        schedule_col = ["ProjektID", "Start produkcji WT", "Zakończenie produkcji", "START1", "1.VA.NAB", "START2",
                        "2.VA.RS", "START3", "3.VA.BKOM", "START4", "4.VA.MKOM", "START5", "5.VA.KOMWKŁ", "START6",
                        "6.VA.MKONC", "START7", "7.VA.OWIE", "START8", "8.VA.MBAT", "CODE"]
        schedule_data = [
            ["D1108610_1-26", datetime.strptime("23/11/2020", "%d/%m/%Y"), datetime.strptime("08/01/2021", "%d/%m/%Y"),
             "1d", 0.0, "2d", 0.798846154, "1d", 2.458461538, "2d", 1.348076923, "1d", 2.685, "2d", 3.174230769, "0d",
             0.0, "1d", 3.995, "???"],
            ["D1108610_2-26", datetime.strptime("23/11/2020", "%d/%m/%Y"), datetime.strptime("08/01/2021", "%d/%m/%Y"),
             "1d", 0, "2d", 5.34, "1d", 8.04, "2d", 1.5, "1d", 0.685, "2d", 6.536, "0d", 7.215332, "1d", 2.9, "???"],
        ]
        self.schedule = pd.DataFrame(columns=schedule_col, data=schedule_data)
        resources_col = ["Shift start", "Shift end", "1.VA.NAB", "2.VA.RS", "3.VA.BKOM", "4.VA.MKOM", "5.VA.KOMWKŁ",
                         "6.VA.MKONC", "7.VA.OWIE", "8.VA.MBAT", "ddd"]
        resources_data = [
            [datetime.strptime("12/11/2020 06:00", "%d/%m/%Y %H:%M"),
             datetime.strptime("12/11/2020 14:00", "%d/%m/%Y %H:%M"), "1x2", "6x1", "4x1", "2x1", "4x1", "2x2", "1x1",
             "2x3", "Thu"],
            [datetime.strptime("12/11/2020 14:00", "%d/%m/%Y %H:%M"),
             datetime.strptime("12/11/2020 22:00", "%d/%m/%Y %H:%M"), "1x2", "6x1", "4x1", "2x1", "4x1", "2x2", "1x1",
             "2x3", "Thu"]
        ]
        self.resources = pd.DataFrame(columns=resources_col, data=resources_data)

    def test_fetch_project(self):
        p = Project(id="D1108610_1-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        self.assertEqual(p, fetch_project(self.schedule.iloc[0]))

    def test_fetch_job(self):
        p = Project(id="D1108610_1-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j1 = Job(duration=timedelta(hours=0.798846154), delay="2d", machine_id="2.VA.RS", project=p)
        j2 = Job(duration=timedelta(hours=2.458461538), delay="1d", machine_id="3.VA.BKOM", project=p)
        j3 = Job(duration=timedelta(hours=1.348076923), delay="2d", machine_id="4.VA.MKOM", project=p)
        j4 = Job(duration=timedelta(hours=2.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p)
        j5 = Job(duration=timedelta(hours=3.174230769), delay="2d", machine_id="6.VA.MKONC", project=p)
        j6 = Job(duration=timedelta(hours=3.995), delay="1d", machine_id="8.VA.MBAT", project=p)
        self.assertEqual([j1, j2, j3, j4, j5, j6], fetch_jobs(self.schedule.iloc[0]))

    def test_fetch_all_jobs(self):
        p1 = Project(id="D1108610_1-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j1 = Job(duration=timedelta(hours=0.798846154), delay="2d", machine_id="2.VA.RS", project=p1)
        j2 = Job(duration=timedelta(hours=2.458461538), delay="1d", machine_id="3.VA.BKOM", project=p1)
        j3 = Job(duration=timedelta(hours=1.348076923), delay="2d", machine_id="4.VA.MKOM", project=p1)
        j4 = Job(duration=timedelta(hours=2.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p1)
        j5 = Job(duration=timedelta(hours=3.174230769), delay="2d", machine_id="6.VA.MKONC", project=p1)
        j6 = Job(duration=timedelta(hours=3.995), delay="1d", machine_id="8.VA.MBAT", project=p1)
        p2 = Project(id="D1108610_2-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j7 = Job(duration=timedelta(hours=5.34), delay="2d", machine_id="2.VA.RS", project=p2)
        j8 = Job(duration=timedelta(hours=8.04), delay="1d", machine_id="3.VA.BKOM", project=p2)
        j9 = Job(duration=timedelta(hours=1.5), delay="2d", machine_id="4.VA.MKOM", project=p2)
        j10 = Job(duration=timedelta(hours=0.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p2)
        j11 = Job(duration=timedelta(hours=6.536), delay="2d", machine_id="6.VA.MKONC", project=p2)
        j12 = Job(duration=timedelta(hours=7.215332), delay="0d", machine_id="7.VA.OWIE", project=p2)
        j13 = Job(duration=timedelta(hours=2.9), delay="1d", machine_id="8.VA.MBAT", project=p2)
        self.assertEqual([j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12, j13], fetch_all_jobs(self.schedule))

    def test_fetch_resources(self):
        r1 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="1.VA.NAB",
                      worker_amount=2)
        r2 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="2.VA.RS",
                      worker_amount=1)
        r3 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="3.VA.BKOM",
                      worker_amount=1)
        r4 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="4.VA.MKOM",
                      worker_amount=1)
        r5 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="5.VA.KOMWKŁ",
                      worker_amount=1)
        r6 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="6.VA.MKONC",
                      worker_amount=2)
        r7 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="7.VA.OWIE",
                      worker_amount=1)
        r8 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="8.VA.MBAT",
                      worker_amount=3)
        self.assertEqual([r1] + [r2] * 6 + [r3] * 4 + [r4] * 2 + [r5] * 4 + [r6] * 2 + [r7] * 1 + [r8] * 2,
                         fetch_resources(self.resources.iloc[0]))

    def test_fetch_all_resources(self):
        r1 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="1.VA.NAB",
                      worker_amount=2)
        r2 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="2.VA.RS",
                      worker_amount=1)
        r3 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="3.VA.BKOM",
                      worker_amount=1)
        r4 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="4.VA.MKOM",
                      worker_amount=1)
        r5 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="5.VA.KOMWKŁ",
                      worker_amount=1)
        r6 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="6.VA.MKONC",
                      worker_amount=2)
        r7 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="7.VA.OWIE",
                      worker_amount=1)
        r8 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14), machine_id="8.VA.MBAT",
                      worker_amount=3)
        r9 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="1.VA.NAB",
                      worker_amount=2)
        r10 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="2.VA.RS",
                       worker_amount=1)
        r11 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="3.VA.BKOM",
                       worker_amount=1)
        r12 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="4.VA.MKOM",
                       worker_amount=1)
        r13 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="5.VA.KOMWKŁ",
                       worker_amount=1)
        r14 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="6.VA.MKONC",
                       worker_amount=2)
        r15 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="7.VA.OWIE",
                       worker_amount=1)
        r16 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22), machine_id="8.VA.MBAT",
                       worker_amount=3)
        self.assertEqual([r1] + [r2] * 6 + [r3] * 4 + [r4] * 2 + [r5] * 4 + [r6] * 2 + [r7] * 1 + [r8] * 2 +
                         [r9] + [r10] * 6 + [r11] * 4 + [r12] * 2 + [r13] * 4 + [r14] * 2 + [r15] * 1 + [r16] * 2,
                         fetch_all_resources(self.resources))