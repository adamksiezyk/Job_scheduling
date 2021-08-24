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
