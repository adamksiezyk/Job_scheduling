from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, order=True)
class Resource:
    """
    A class that represents a resource entity
    """
    start_dt: datetime  # Resource start date
    end_dt: datetime  # Resource end date
    machine_id: str  # Resource machine ID
    worker_amount: int  # Resource workers amount
