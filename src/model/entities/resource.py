import functools
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True, order=True)
class Resource:
    """
    A class that represents a resource entity
    """
    start_dt: datetime  # Resource start date
    end_dt: datetime  # Resource end date
    worker_amount: int  # Resource workers amount

    def __post_init__(self):
        if self.start_dt > self.end_dt:
            raise ValueError("Resource end date can not be earlier than resource start date")


class Resources:
    """
    A container class for resources
    """

    def __init__(self, resources: dict[str, list[Resource]]):
        self.__resources = resources

    def get_resources(self, machine_id: str) -> list[Resource]:
        try:
            return self.__resources[machine_id]
        except KeyError:
            return []

    def get_resource(self, machine_id: str, resource_idx: int) -> Resource:
        try:
            return self.__resources[machine_id][resource_idx]
        except KeyError:
            raise ValueError("Resource not found")

    def get_all_resources(self) -> list[Resource]:
        return functools.reduce(lambda ans, elem: ans + elem, self.__resources.values(), [])

    def get_resources_grouped_by_machine(self):
        return functools.reduce(lambda ans, elem: ans + [elem], self.__resources.values(), [])

    def get_earliest_resource(self, machine_id: str):
        try:
            return self.__resources[machine_id][0]
        except KeyError:
            raise ValueError("No resources available")

    def get_machine_ids(self) -> list[str]:
        return list(self.__resources.keys())


class UsedResources:
    """
    Container class for used resources
    """

    def __init__(self, resources: Resources):
        self.__resources = resources
        self.__used_resources = self.__create_initial_used_resources(resources)

    @staticmethod
    def __create_initial_used_resources(resources: Resources) -> dict[str, dict[int, Optional[Resource]]]:
        return {machine_id: {} for machine_id in resources.get_machine_ids()}

    def mark_resource_as_used(self, machine_id: str, resource_idx: int) -> None:
        self.__used_resources[machine_id][resource_idx] = None

    def use_resource(self, machine_id: str, resource_idx: int, new_resource: Resource) -> None:
        self.__used_resources[machine_id][resource_idx] = new_resource

    def get_resources(self, machine_id: str) -> list[Resource]:
        return [self.get_resource(machine_id, i) for i in range(len(self.__resources.get_resources(machine_id)))]

    def get_resource(self, machine_id: str, resource_idx: int) -> Resource:
        try:
            return self.__used_resources[machine_id][resource_idx]
        except KeyError:
            return self.__resources.get_resource(machine_id, resource_idx)

    def find_earliest_resource(self, machine_id: str, start_dt: datetime) -> tuple[int, Resource]:
        """
        Returns the earliest available resource for the given machine and start datetime
        @param machine_id: machine ID
        @param start_dt: start datetime
        @return: earliest available resource and it's index
        @raise ValueError: when no resource was found
        """
        # Use a generator to stop looping after first available resource is found
        try:
            return next(((i, resource) for i, resource in enumerate(self.get_resources(machine_id))
                         if self.check_if_start_dt_is_in_resource(start_dt, resource)))
        except (KeyError, StopIteration):
            raise ValueError("No resources available")

    @staticmethod
    def check_if_start_dt_is_in_resource(start_dt: datetime, resource: Resource) -> bool:
        """
        Returns True if start datetime is in resource
        @param start_dt: start datetime
        @param resource: resource
        @return: True if start datetime is in resource
        """
        return resource is not None and start_dt < resource.end_dt
