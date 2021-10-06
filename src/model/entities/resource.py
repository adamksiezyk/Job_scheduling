import functools
from dataclasses import dataclass
from datetime import datetime


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

    def __init__(self):
        self.__resources: dict[str, list[Resource]] = dict()

    def append(self, machine_id: str, resource: Resource) -> None:
        if machine_id in self.__resources:
            self.__resources[machine_id].append(resource)
        else:
            self.__resources[machine_id] = [resource]

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
