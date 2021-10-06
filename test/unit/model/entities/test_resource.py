from datetime import datetime
from unittest import TestCase

from src.model.entities.resource import Resource, Resources


class TestResource(TestCase):
    def test_post_init(self):
        init_args = {
            'start_dt': datetime(2021, 3, 28),
            'end_dt': datetime(2021, 3, 20),
            'worker_amount': 2
        }
        self.assertRaises(ValueError, Resource, **init_args)


class TestResources(TestCase):
    def setUp(self) -> None:
        self.resource1 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                                  worker_amount=2)
        self.resource2 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22),
                                  worker_amount=2)
        self.resource3 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6),
                                  worker_amount=2)
        self.resource4 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                                  worker_amount=2)
        self.resource5 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22),
                                  worker_amount=2)
        self.resource6 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6),
                                  worker_amount=2)
        self.resources = Resources()
        self.machine_id_1 = "M1"
        self.machine_id_2 = "M2"

    def append_resources(self):
        self.resources.append(self.machine_id_1, self.resource1)
        self.resources.append(self.machine_id_1, self.resource2)
        self.resources.append(self.machine_id_1, self.resource3)
        self.machine_1_resources = [self.resource1, self.resource2, self.resource3]
        self.resources.append(self.machine_id_2, self.resource4)
        self.resources.append(self.machine_id_2, self.resource5)
        self.resources.append(self.machine_id_2, self.resource6)
        self.machine_2_resources = [self.resource4, self.resource5, self.resource6]

    def test_append_and_get_resources(self):
        self.append_resources()
        self.assertEqual(self.machine_1_resources, self.resources.get_resources(self.machine_id_1))
        self.assertEqual(self.machine_2_resources, self.resources.get_resources(self.machine_id_2))

    def test_get_resources_no_resources_available(self):
        self.assertEqual([], self.resources.get_resources("M0"))

    def test_get_resource(self):
        self.append_resources()
        resource_idx = 1
        self.assertEqual(self.machine_1_resources[resource_idx],
                         self.resources.get_resource(self.machine_id_1, resource_idx))

    def test_get_all_resources(self):
        self.append_resources()
        all_resources = self.machine_1_resources + self.machine_2_resources
        self.assertEqual(all_resources, self.resources.get_all_resources())

    def test_get_all_resources_no_resources_available(self):
        self.assertEqual([], self.resources.get_all_resources())

    def test_get_resources_grouped_by_machine(self):
        self.append_resources()
        resources_grouped_by_machine = [self.machine_1_resources, self.machine_2_resources]
        self.assertEqual(resources_grouped_by_machine, self.resources.get_resources_grouped_by_machine())

    def test_get_earliest_resource(self):
        self.append_resources()
        self.assertEqual(self.machine_1_resources[0], self.resources.get_earliest_resource(self.machine_id_1))
        self.assertEqual(self.machine_2_resources[0], self.resources.get_earliest_resource(self.machine_id_2))

    def test_get_earliest_resource_no_resources_available(self):
        self.assertRaises(ValueError, self.resources.get_earliest_resource, self.machine_id_1)

    def test_get_machine_ids(self):
        self.append_resources()
        self.assertEqual([self.machine_id_1, self.machine_id_2], self.resources.get_machine_ids())
