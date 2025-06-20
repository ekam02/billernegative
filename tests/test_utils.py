from datetime import date

from pandas import DataFrame

from utils import create_documents, find_biller_memos_by_numbers, find_jano_partner_by_attributes, read_negative_invoices
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

    def test_find_jano_partner_by_attributes(self):
        jano_partner = find_jano_partner_by_attributes(
            line=2, store=3814, pos=17, trx=7294,
            billed_at=date(2025, 6, 7)
        )
        if not jano_partner:
            assert isinstance(jano_partner, Document)
        else:
            assert False

    def test_find_biller_memos_by_numbers(self):
        biller_memos = find_biller_memos_by_numbers(numbers=["VCJD2027339"])
        if biller_memos:
            assert isinstance(biller_memos, list)
            assert isinstance(biller_memos[0], Document)
        else:
            assert False

        biller_memos = find_biller_memos_by_numbers(numbers=["VCSU1046072", "VCJ82014804"])
        if biller_memos:
            assert isinstance(biller_memos, list)
            assert isinstance(biller_memos[0], Document)
        else:
            assert False

    def test_validate_document(self):
        pass

    def test_validate_documents(self):
        pass

    def test_create_report(self):
        pass
