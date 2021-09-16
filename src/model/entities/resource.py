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
