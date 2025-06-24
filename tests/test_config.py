from datetime import timedelta

from config import settings


class TestConfig:
    def test_set_start_date(self):
        new_start_date = settings.start_date + timedelta(days=1)
        settings.set_start_date(new_start_date)
        assert settings.start_date == new_start_date

    def test_set_end_date(self):
        new_end_date = settings.end_date - timedelta(days=1)
        settings.set_end_date(new_end_date)
        assert settings.end_date == new_end_date
