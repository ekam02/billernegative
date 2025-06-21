from datetime import date

from pandas import DataFrame

from utils import create_document, create_documents, find_biller_memos_by_numbers, find_biller_replace_by_attributes, \
    find_biller_replaces_from_documents, find_biller_replace_by_document, find_jano_partner_by_attributes, \
    read_negative_invoices, validate_document, validate_documents
from schemas import Document


class TestFinder:
    def test_read_negative_invoices(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        assert isinstance(negative_invoices, DataFrame)

    def test_find_jano_partner_by_attributes(self):
        jano_partner = find_jano_partner_by_attributes(
            line=2, store=3814, pos=17, trx=7294,
            billed_at=date(2025, 6, 7)
        )
        if jano_partner:
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

    def test_find_biller_replace_by_attributes(self):
        biller_replace = find_biller_replace_by_attributes(
            line=1, store=23, pos=17, trx=9726,
            billed_at=date(2025, 5, 27)
        )
        if biller_replace:
            assert isinstance(biller_replace, Document)
        else:
            assert False

    def test_find_biller_replace_by_document(self):
        biller_replace = find_biller_replace_by_document(
            Document(
                store_name="JUMBO CARRERA 30 (16)",
                doc_num="VCSU1046274",
                doc_type=9,
                line=1,
                store=16,
                pos=17,
                trx=530,
                billed_at=date(2025, 6, 12),
                sent_at=date(2025, 6, 12),
                amount=-189552,
                customer=900965992,
                duplicated=None,
                status="OK",
                log_dian="",
                uuid="151371",
                memos=None,
                pair_up=None,
                replaces=None,
                evaluation="",
            )
        )
        assert isinstance(biller_replace, Document)

    def test_find_biller_replaces_from_documents(self):
        biller_replaces = find_biller_replaces_from_documents(
            [
                Document(
                    store_name="JUMBO CARRERA 30 (16)",
                    doc_num="VCSU1046274",
                    doc_type=9,
                    line=1,
                    store=16,
                    pos=17,
                    trx=530,
                    billed_at=date(2025, 6, 12),
                    sent_at=date(2025, 6, 12),
                    amount=-189552,
                    customer=900965992,
                    duplicated=None,
                    status="OK",
                    log_dian="",
                    uuid="336831",
                    memos=None,
                    pair_up=None,
                    replaces=None,
                    evaluation="",
                ),
                Document(
                    store_name="JUMBO CARRERA 30 (16)",
                    doc_num="VCJ42011654",
                    doc_type=9,
                    line=1,
                    store=16,
                    pos=40,
                    trx=9310,
                    billed_at=date(2025, 6, 3),
                    sent_at=date(2025, 6, 3),
                    amount=-24830,
                    customer=900965992,
                    duplicated=None,
                    status="OK",
                    log_dian="",
                    uuid="fb4ad0",
                    memos=None,
                    pair_up=None,
                    replaces=None,
                    evaluation="",
                ),
            ]
        )
        if biller_replaces:
            assert isinstance(biller_replaces, list)
            assert isinstance(biller_replaces[0], Document)
        else:
            assert False


class TestMaker:
    def test_create_document(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        document = create_document(row=negative_invoices.iloc[0])
        if document:
            assert isinstance(document, Document)
        else:
            assert False

    def test_create_documents(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        documents = create_documents(df=negative_invoices)
        if documents:
            assert isinstance(documents, list)
            assert isinstance(documents[0], Document)
        else:
            assert False

    def test_validate_document(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        document = create_document(row=negative_invoices.iloc[0])
        validate = validate_document(document)
        if validate:
            assert isinstance(validate, str)
        else:
            assert False

    def test_validate_documents(self):
        negative_invoices = read_negative_invoices(start_date=date(2025, 6, 1), end_date=date(2025, 6, 15))
        documents = create_documents(df=negative_invoices)
        documents = validate_documents(documents)
        if documents:
            assert isinstance(documents, list)
            assert isinstance(documents[0], Document)
        else:
            assert False

    def test_create_report(self):
        pass
