import unittest
from datetime import datetime, timedelta

import pandas as pd

from src.model.db.excel.fetch import fetch_project, fetch_jobs_in_project, fetch_jobs_list, fetch_resources, \
    fetch_all_resources, fetch_jobs_dict, fetch_jobs_dict_from_list
from src.model.entities.job import Job
from src.model.entities.project import Project
from src.model.entities.resource import Resource, Resources


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

    def test_fetch_jobs_in_project(self):
        p = Project(id="D1108610_1-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j1 = Job(duration=timedelta(hours=0), delay="1d", machine_id="1.VA.NAB", project=p,
                 previous_machines=[])
        j2 = Job(duration=timedelta(hours=0.798846154), delay="2d", machine_id="2.VA.RS", project=p,
                 previous_machines=["1.VA.NAB"])
        j3 = Job(duration=timedelta(hours=2.458461538), delay="1d", machine_id="3.VA.BKOM", project=p,
                 previous_machines=[])
        j4 = Job(duration=timedelta(hours=1.348076923), delay="2d", machine_id="4.VA.MKOM", project=p,
                 previous_machines=["3.VA.BKOM"])
        j5 = Job(duration=timedelta(hours=2.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'])
        j6 = Job(duration=timedelta(hours=3.174230769), delay="2d", machine_id="6.VA.MKONC", project=p,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'])
        j7 = Job(duration=timedelta(hours=0), delay="0d", machine_id="7.VA.OWIE", project=p,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'])
        j8 = Job(duration=timedelta(hours=3.995), delay="1d", machine_id="8.VA.MBAT", project=p,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC',
                                    '7.VA.OWIE'])
        self.assertEqual([j1, j2, j3, j4, j5, j6, j7, j8], fetch_jobs_in_project(self.schedule.iloc[0]))

    def test_fetch_jobs_list(self):
        p1 = Project(id="D1108610_1-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j1 = Job(duration=timedelta(hours=0), delay="1d", machine_id="1.VA.NAB", project=p1,
                 previous_machines=[])
        j2 = Job(duration=timedelta(hours=0.798846154), delay="2d", machine_id="2.VA.RS", project=p1,
                 previous_machines=["1.VA.NAB"])
        j3 = Job(duration=timedelta(hours=2.458461538), delay="1d", machine_id="3.VA.BKOM", project=p1,
                 previous_machines=[])
        j4 = Job(duration=timedelta(hours=1.348076923), delay="2d", machine_id="4.VA.MKOM", project=p1,
                 previous_machines=["3.VA.BKOM"])
        j5 = Job(duration=timedelta(hours=2.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'])
        j6 = Job(duration=timedelta(hours=3.174230769), delay="2d", machine_id="6.VA.MKONC", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'])
        j7 = Job(duration=timedelta(hours=0), delay="0d", machine_id="7.VA.OWIE", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'])
        j8 = Job(duration=timedelta(hours=3.995), delay="1d", machine_id="8.VA.MBAT", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC',
                                    '7.VA.OWIE'])
        p2 = Project(id="D1108610_2-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j9 = Job(duration=timedelta(hours=0), delay="1d", machine_id="1.VA.NAB", project=p2,
                 previous_machines=[])
        j10 = Job(duration=timedelta(hours=5.34), delay="2d", machine_id="2.VA.RS", project=p2,
                  previous_machines=["1.VA.NAB"])
        j11 = Job(duration=timedelta(hours=8.04), delay="1d", machine_id="3.VA.BKOM", project=p2,
                  previous_machines=[])
        j12 = Job(duration=timedelta(hours=1.5), delay="2d", machine_id="4.VA.MKOM", project=p2,
                  previous_machines=["3.VA.BKOM"])
        j13 = Job(duration=timedelta(hours=0.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'])
        j14 = Job(duration=timedelta(hours=6.536), delay="2d", machine_id="6.VA.MKONC", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'])
        j15 = Job(duration=timedelta(hours=7.215332), delay="0d", machine_id="7.VA.OWIE", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'])
        j16 = Job(duration=timedelta(hours=2.9), delay="1d", machine_id="8.VA.MBAT", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC',
                                     '7.VA.OWIE'])
        self.assertEqual([j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12, j13, j14, j15, j16],
                         fetch_jobs_list(self.schedule))

    def test_fetch_jobs_dict(self):
        p1 = Project(id="D1108610_1-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j1 = Job(duration=timedelta(hours=0), delay="1d", machine_id="1.VA.NAB", project=p1,
                 previous_machines=[])
        j2 = Job(duration=timedelta(hours=0.798846154), delay="2d", machine_id="2.VA.RS", project=p1,
                 previous_machines=["1.VA.NAB"])
        j3 = Job(duration=timedelta(hours=2.458461538), delay="1d", machine_id="3.VA.BKOM", project=p1,
                 previous_machines=[])
        j4 = Job(duration=timedelta(hours=1.348076923), delay="2d", machine_id="4.VA.MKOM", project=p1,
                 previous_machines=["3.VA.BKOM"])
        j5 = Job(duration=timedelta(hours=2.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'])
        j6 = Job(duration=timedelta(hours=3.174230769), delay="2d", machine_id="6.VA.MKONC", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'])
        j7 = Job(duration=timedelta(hours=0), delay="0d", machine_id="7.VA.OWIE", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'])
        j8 = Job(duration=timedelta(hours=3.995), delay="1d", machine_id="8.VA.MBAT", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC',
                                    '7.VA.OWIE'])
        p2 = Project(id="D1108610_2-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j9 = Job(duration=timedelta(hours=0), delay="1d", machine_id="1.VA.NAB", project=p2,
                 previous_machines=[])
        j10 = Job(duration=timedelta(hours=5.34), delay="2d", machine_id="2.VA.RS", project=p2,
                  previous_machines=["1.VA.NAB"])
        j11 = Job(duration=timedelta(hours=8.04), delay="1d", machine_id="3.VA.BKOM", project=p2,
                  previous_machines=[])
        j12 = Job(duration=timedelta(hours=1.5), delay="2d", machine_id="4.VA.MKOM", project=p2,
                  previous_machines=["3.VA.BKOM"])
        j13 = Job(duration=timedelta(hours=0.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'])
        j14 = Job(duration=timedelta(hours=6.536), delay="2d", machine_id="6.VA.MKONC", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'])
        j15 = Job(duration=timedelta(hours=7.215332), delay="0d", machine_id="7.VA.OWIE", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'])
        j16 = Job(duration=timedelta(hours=2.9), delay="1d", machine_id="8.VA.MBAT", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC',
                                     '7.VA.OWIE'])
        jobs_dict = {
            p1: [j1, j2, j3, j4, j5, j6, j7, j8],
            p2: [j9, j10, j11, j12, j13, j14, j15, j16]
        }
        self.assertEqual(jobs_dict, fetch_jobs_dict(self.schedule))

    def test_fetch_jobs_dict_from_list(self):
        p1 = Project(id="D1108610_1-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j1 = Job(duration=timedelta(hours=0), delay="1d", machine_id="1.VA.NAB", project=p1,
                 previous_machines=[])
        j2 = Job(duration=timedelta(hours=0.798846154), delay="2d", machine_id="2.VA.RS", project=p1,
                 previous_machines=["1.VA.NAB"])
        j3 = Job(duration=timedelta(hours=2.458461538), delay="1d", machine_id="3.VA.BKOM", project=p1,
                 previous_machines=[])
        j4 = Job(duration=timedelta(hours=1.348076923), delay="2d", machine_id="4.VA.MKOM", project=p1,
                 previous_machines=["3.VA.BKOM"])
        j5 = Job(duration=timedelta(hours=2.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'])
        j6 = Job(duration=timedelta(hours=3.174230769), delay="2d", machine_id="6.VA.MKONC", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'])
        j7 = Job(duration=timedelta(hours=0), delay="0d", machine_id="7.VA.OWIE", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'])
        j8 = Job(duration=timedelta(hours=3.995), delay="1d", machine_id="8.VA.MBAT", project=p1,
                 previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC',
                                    '7.VA.OWIE'])
        p2 = Project(id="D1108610_2-26", start_dt=datetime(2020, 11, 23, 6), expiration_dt=datetime(2021, 1, 8))
        j9 = Job(duration=timedelta(hours=0), delay="1d", machine_id="1.VA.NAB", project=p2,
                 previous_machines=[])
        j10 = Job(duration=timedelta(hours=5.34), delay="2d", machine_id="2.VA.RS", project=p2,
                  previous_machines=["1.VA.NAB"])
        j11 = Job(duration=timedelta(hours=8.04), delay="1d", machine_id="3.VA.BKOM", project=p2,
                  previous_machines=[])
        j12 = Job(duration=timedelta(hours=1.5), delay="2d", machine_id="4.VA.MKOM", project=p2,
                  previous_machines=["3.VA.BKOM"])
        j13 = Job(duration=timedelta(hours=0.685), delay="1d", machine_id="5.VA.KOMWKŁ", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM'])
        j14 = Job(duration=timedelta(hours=6.536), delay="2d", machine_id="6.VA.MKONC", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ'])
        j15 = Job(duration=timedelta(hours=7.215332), delay="0d", machine_id="7.VA.OWIE", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC'])
        j16 = Job(duration=timedelta(hours=2.9), delay="1d", machine_id="8.VA.MBAT", project=p2,
                  previous_machines=['1.VA.NAB', '2.VA.RS', '3.VA.BKOM', '4.VA.MKOM', '5.VA.KOMWKŁ', '6.VA.MKONC',
                                     '7.VA.OWIE'])
        jobs_list = [j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12, j13, j14, j15, j16]
        jobs_dict = {
            p1: [j1, j2, j3, j4, j5, j6, j7, j8],
            p2: [j9, j10, j11, j12, j13, j14, j15, j16]
        }
        self.assertEqual(jobs_dict, fetch_jobs_dict_from_list(jobs_list))

    def test_fetch_resources(self):
        r1 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=2)
        r2 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r3 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r4 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r5 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r6 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=2)
        r7 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r8 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=3)
        expected_resources = {
            "1.VA.NAB": [r1],
            "2.VA.RS": [r2] * 6,
            "3.VA.BKOM": [r3] * 4,
            "4.VA.MKOM": [r4] * 2,
            "5.VA.KOMWKŁ": [r5] * 4,
            "6.VA.MKONC": [r6] * 2,
            "7.VA.OWIE": [r7],
            "8.VA.MBAT": [r8] * 2,
        }
        resources = Resources()
        fetch_resources(resources, self.resources.iloc[0])
        for key, value in expected_resources.items():
            self.assertEqual(value, resources.get_resources(key))

    def test_fetch_all_resources(self):
        r1 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=2)
        r2 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r3 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r4 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r5 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r6 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=2)
        r7 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=1)
        r8 = Resource(start_dt=datetime(2020, 11, 12, 6), end_dt=datetime(2020, 11, 12, 14),
                      worker_amount=3)
        r9 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                      worker_amount=2)
        r10 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                       worker_amount=1)
        r11 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                       worker_amount=1)
        r12 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                       worker_amount=1)
        r13 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                       worker_amount=1)
        r14 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                       worker_amount=2)
        r15 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                       worker_amount=1)
        r16 = Resource(start_dt=datetime(2020, 11, 12, 14), end_dt=datetime(2020, 11, 12, 22),
                       worker_amount=3)
        expected_resources = {
            "1.VA.NAB": [r1, r9],
            "2.VA.RS": [r2] * 6 + [r10] * 6,
            "3.VA.BKOM": [r3] * 4 + [r11] * 4,
            "4.VA.MKOM": [r4] * 2 + [r12] * 2,
            "5.VA.KOMWKŁ": [r5] * 4 + [r13] * 4,
            "6.VA.MKONC": [r6] * 2 + [r14] * 2,
            "7.VA.OWIE": [r7, r15],
            "8.VA.MBAT": [r8] * 2 + [r16] * 2
        }
        resources = fetch_all_resources(self.resources)
        for key, values in expected_resources.items():
            self.assertEqual(values, resources.get_resources(key))
