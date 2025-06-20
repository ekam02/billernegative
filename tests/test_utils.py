from datetime import date

from pandas import DataFrame

from utils import create_documents, read_negative_invoices
from schemas import Document


class TestUtils:
    def test_read_negative_invoices(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        assert isinstance(negative_invoices, DataFrame)

    def test_create_documents(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        documents = create_documents(df=negative_invoices)
        if documents:
            assert isinstance(documents, list)
            assert isinstance(documents[0], Document)
        else:
            assert False

    def test_validate_document(self):
        pass

    def test_validate_documents(self):
        pass

    def test_create_report(self):
        pass
