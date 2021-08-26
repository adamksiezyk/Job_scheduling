from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, order=True)
class Project:
    """
    Class that represents a single project entity
    """
    start_dt: datetime
    expiration_dt: datetime
    id: str

    def __post_init__(self):
        if self.start_dt > self.expiration_dt:
            raise ValueError("Project expiration date can not be earlier than project start date")
