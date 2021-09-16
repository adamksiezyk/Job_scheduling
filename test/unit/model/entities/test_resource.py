from datetime import datetime
from unittest import TestCase

from src.model.entities.resource import Resource


class TestResource(TestCase):
    def test_post_init(self):
        init_args = {
            'start_dt': datetime(2021, 3, 28),
            'end_dt': datetime(2021, 3, 20),
            'worker_amount': 2
        }
        self.assertRaises(ValueError, Resource, **init_args)
