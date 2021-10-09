from dataclasses import replace
from datetime import datetime, timedelta
from typing import Type, Union
from unittest import TestCase

from src.model.entities.resource import Resource, Resources, UsedResources

Container = Union[Type[Resources], Type[UsedResources]]


class ITestResources(TestCase):
    def set_up(self) -> None:
        self.machine1_id = "M1"
        self.machine2_id = "M2"
        resource1 = Resource(start_dt=datetime(2021, 4, 1, 6), end_dt=datetime(2021, 4, 1, 14),
                             worker_amount=2)
        resource2 = Resource(start_dt=datetime(2021, 4, 1, 14), end_dt=datetime(2021, 4, 1, 22),
                             worker_amount=2)
        resource3 = Resource(start_dt=datetime(2021, 4, 1, 22), end_dt=datetime(2021, 4, 2, 6),
                             worker_amount=2)
        self.machine1_resources = [resource1, resource2, resource3]
        self.machine2_resources = [resource1, resource2, resource3]
        resources = {
            self.machine1_id: self.machine1_resources,
            self.machine2_id: self.machine2_resources
        }
        self.resources = Resources(resources)


class TestResource(TestCase):
    def test_post_init(self):
        init_args = {
            'start_dt': datetime(2021, 3, 28),
            'end_dt': datetime(2021, 3, 20),
            'worker_amount': 2
        }
        self.assertRaises(ValueError, Resource, **init_args)


class TestResources(ITestResources):
    def setUp(self) -> None:
        self.set_up()

    def test_init_and_get_resources(self):
        self.assertEqual(self.machine1_resources, self.resources.get_resources(self.machine1_id))
        self.assertEqual(self.machine2_resources, self.resources.get_resources(self.machine2_id))

    def test_get_resources_no_resources_available(self):
        self.assertEqual([], self.resources.get_resources("M0"))

    def test_get_resource(self):
        resource_idx = 1
        self.assertEqual(self.machine1_resources[resource_idx],
                         self.resources.get_resource(self.machine1_id, resource_idx))

    def test_get_all_resources(self):
        all_resources = self.machine1_resources + self.machine2_resources
        self.assertEqual(all_resources, self.resources.get_all_resources())

    def test_get_all_resources_no_resources_available(self):
        resources = Resources({})
        self.assertEqual([], resources.get_all_resources())

    def test_get_resources_grouped_by_machine(self):
        resources_grouped_by_machine = [self.machine1_resources, self.machine2_resources]
        self.assertEqual(resources_grouped_by_machine, self.resources.get_resources_grouped_by_machine())

    def test_get_earliest_resource(self):
        self.assertEqual(self.machine1_resources[0], self.resources.get_earliest_resource(self.machine1_id))
        self.assertEqual(self.machine2_resources[0], self.resources.get_earliest_resource(self.machine2_id))

    def test_get_earliest_resource_no_resources_available(self):
        resources = Resources({})
        self.assertRaises(ValueError, resources.get_earliest_resource, self.machine1_id)

    def test_get_machine_ids(self):
        self.assertEqual([self.machine1_id, self.machine2_id], self.resources.get_machine_ids())


class TestUsedResources(ITestResources):
    def setUp(self) -> None:
        self.set_up()
        self.used_resources = UsedResources(self.resources)

    def test_init_and_get_resources_all_available(self):
        self.assertEqual(self.machine1_resources, self.used_resources.get_resources(self.machine1_id))
        self.assertEqual(self.machine2_resources, self.used_resources.get_resources(self.machine2_id))

    def test_mark_resource_as_used_and_use_resource_and_get_resource_and_get_resources(self):
        expected_results = [*self.machine1_resources]
        unavailable_resource = 0
        expected_results[unavailable_resource] = None
        used_resource = 2
        new_resource = replace(expected_results[used_resource],
                               start_dt=expected_results[used_resource].start_dt + timedelta(hours=2))
        expected_results[used_resource] = new_resource

        self.used_resources.mark_resource_as_used(self.machine1_id, unavailable_resource)
        self.used_resources.use_resource(self.machine1_id, used_resource, new_resource)

        self.assertEqual(None, self.used_resources.get_resource(self.machine1_id, unavailable_resource))
        self.assertEqual(new_resource, self.used_resources.get_resource(self.machine1_id, used_resource))
        self.assertEqual(expected_results, self.used_resources.get_resources(self.machine1_id))

    def test_find_earliest_resource_not_found(self):
        empty_used_resources = UsedResources(Resources({}))
        machine_id = "M1"
        start_dt = self.used_resources.get_resource(machine_id, 0).start_dt + timedelta(hours=1)
        self.assertRaises(ValueError, empty_used_resources.find_earliest_resource, machine_id, start_dt)

        machine_id = "M0"
        start_dt = self.used_resources.get_resource("M1", 0).start_dt + timedelta(hours=0)
        self.assertRaises(ValueError, self.used_resources.find_earliest_resource, machine_id, start_dt)

        machine_id = "M1"
        start_dt = self.used_resources.get_resource(machine_id, 0).start_dt + timedelta(days=20)
        self.assertRaises(ValueError, self.used_resources.find_earliest_resource, machine_id, start_dt)

    def test_find_earliest_resource_found(self):
        machine_id = "M1"
        start_dt = self.used_resources.get_resource(machine_id, 0).start_dt + timedelta(hours=2)
        found_resource = self.machine1_resources[0]
        self.assertEqual((0, found_resource), self.used_resources.find_earliest_resource(machine_id, start_dt))
