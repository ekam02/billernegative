from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pandas import DataFrame, Series

from .finder import find_biller_memos_by_numbers, find_biller_replaces_from_documents, find_jano_partner_by_attributes
from config import headers, log_console_level, log_file_level, output_dir, report_name, settings
from config.logger_config import setup_logger
from schemas import Document


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def cast_document_as_report(document: Document, heads: list = headers) -> Optional[dict]:
    try:
        if not isinstance(document, Document):
            raise TypeError("The 'document' is expected to be of type 'Document' from models.")
        return {
            heads[0]: document.doc_num,  # factura
            heads[1]: document.doc_type,  # c_origen
            heads[2]: document.store_name,  # tienda
            heads[3]: document.store,  # no_tienda
            heads[4]: document.pos,  # caja
            heads[5]: document.trx,  # trx
            heads[6]: document.billed_at,  # fecha
            heads[7]: "|".join(document.get_memo_doc_nums()) if document.get_memo_doc_nums() else "",  # notas
            heads[8]: document.amount,  # total_factura
            heads[9]: document.get_memo_total_amount(),  # total_notas
            heads[10]: document.get_amount_difference_with_memos(),  # sub_total
            heads[11]: document.customer,  # cliente
            heads[12]: document.uuid,  # cufe
            heads[13]: document.partner.doc_num if document.partner else None,  # factura_jano
            heads[14]: "|".join(document.get_replace_doc_nums()) if document.get_replace_doc_nums() else "",  # remplazos
            heads[15]: document.get_replace_total_amount(),  # valor_remplazos
            heads[16]: document.get_total_amount_with_replaces(),  # total
            heads[17]: document.evaluation  # evaluación
        }
    except TypeError as e:
        logger.exception(f"The types provided are not correct. {e}")
    except Exception as e:
        logger.exception(f"An error occurred while casting a document as a report: {e}")
    return None


def create_document(row: Series) -> Optional[Document]:
    try:
        if not isinstance(row, Series):
            raise TypeError("The 'row' is expected to be of type 'Series' from Pandas.")
        if row.empty:
            raise ValueError("The 'row' is empty.")
        document = Document(**row.to_dict())
        document.memos = find_biller_memos_by_numbers(row["memo_lts"].split("|"))
        document.partner = find_jano_partner_by_attributes(
            document.line, document.store, document.pos, document.trx, document.billed_at,
        )
        document.replaces = find_biller_replaces_from_documents(document.memos) if document.memos else None
        document.evaluation = validate_document(document)
        # logger.info(f"Document created successfully: {document}")
        return document
    except Exception as e:
        logger.exception(f"An error occurred while creating a document: {e}")
    return None


def create_documents(df: DataFrame) -> Optional[List[Document]]:
    try:
        if not isinstance(df, DataFrame):
            raise TypeError("The 'df' is expected to be of type 'DataFrame' from Pandas.")
        if df.empty:
            raise ValueError("The 'df' is empty.")
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(create_document, row) for _, row in df.iterrows()]
        documents = [future.result() for future in as_completed(futures)]
        # logger.info(f"Documents created successfully: {documents}")
        return documents
    except TypeError as e:
        logger.exception(f"The types provided are not correct. {e}")
    except ValueError as e:
        logger.exception(f"An error occurred while creating documents: {e}")
    except Exception as e:
        logger.exception(f"An error occurred while creating documents: {e}")
    return None


def get_report_name() -> Path:
    return output_dir / f"{report_name}_{settings.start_date}_{settings.end_date}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"


def validate_document(document: Document) -> Optional[str]:
    try:
        if not isinstance(document, Document):
            raise TypeError("The 'document' is expected to be of type 'Document' from models.")
        # Evalúa si el documento tiene pareja en Jano.
        if document.partner:
            # Evalúa si el número del documento es igual al número de la pajera en Jano.
            if document.doc_num == document.partner.doc_num:  # Números de facturas iguales
                # Evalúa si alguna de las fechas de sus notas es más antigua a la fecha del documento
                if any([memo.billed_at < document.billed_at for memo in document.memos]):
                    return "Error En Relación Factura-Nota(s)"
                # Evalúa si `sub_total >= 0`
                elif document.get_amount_difference_with_memos() >= 0:
                    return "OK"
                else:
                    # Como `sub_total < 0`; evalúa si existen remplazos
                    if document.replaces:
                        # Evalúa si el `total >= 0`. `total` es la suma de `sub_total` y `valor_remplazo`.
                        if document.get_total_amount_with_replaces() >= 0:
                            return "Remplazo OK"
                        # Como `total < 0`
                        else:
                            return "Error Remplazo"
                    # Cómo no hay remplazos
                    else:
                        return "Error POS"
            # Como los números son diferentes entre el documento y su pareja.
            else:
                return "Error Prefijo Factura"
        # Como no tiene pareja en Jano
        else:
            return "Sin Factura En Jano"
    except Exception as e:
        logger.exception(f"An error occurred while validating a document: {e}")
    return None


def validate_documents(documents: List[Document]) -> Optional[List[Document]]:
    try:
        if not isinstance(documents, list):
            raise TypeError("It is expected that 'documents' is of type 'list'.")
        if not isinstance(documents[0], Document):
            raise TypeError("It is expected that 'documents' is of type 'list' of 'Document'.")
        for document in documents:
            document.evaluation = validate_document(document)
        return documents
    except Exception as e:
        logger.exception(f"An error occurred while validating documents: {e}")
    return None


def create_report(documents: List[Document], name: Path = get_report_name()) -> bool:
    report_df = DataFrame([cast_document_as_report(document) for document in documents])
    if not report_df.empty:
        report_df.to_csv(name, index=False, encoding="utf-8", sep=";")
        return True
    return False
