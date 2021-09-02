from datetime import datetime
from unittest import TestCase

from src.model.entities.project import Project


class TestProject(TestCase):
    def test_post_init(self):
        init_args = {'id': "P1", 'start_dt': datetime(2021, 3, 28), 'expiration_dt': datetime(2021, 3, 20)}
        self.assertRaises(ValueError, Project, **init_args)
