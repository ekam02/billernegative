from datetime import date

from pandas import DataFrame

from utils import read_negative_invoices


class TestUtils:
    def test_read_negative_invoices(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        assert isinstance(negative_invoices, DataFrame)

    def test_create_documents(self):
        # Crear Documents desde un DataFrame
        pass

    def test_validate_document(self):
        pass

    def test_validate_documents(self):
        pass

    def test_create_report(self):
        pass
