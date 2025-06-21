from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from pandas import DataFrame, Series

from .finder import find_biller_memos_by_numbers, find_biller_replaces_from_documents, find_jano_partner_by_attributes
from config import log_console_level, log_file_level
from config.logger_config import setup_logger
from schemas import Document


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


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


def validate_document(document: Document) -> Optional[str]:
    try:
        if not isinstance(document, Document):
            raise TypeError("The 'document' is expected to be of type 'Document' from models.")
        if document.partner:
            if document.doc_num != document.partner.doc_num:
                return "Error Prefijo Factura"
            elif document.doc_num == document.partner.doc_num:  # Números de facturas iguales
                # Si el sub_total es >= 0, entonces "OK"
                if document.get_amount_difference_with_memos() >= 0:
                    return "OK"
                else:  # Si el sub_total es < 0, entonces:
                    # Si en el juego de documentos hay remplazos, entonces:
                    if document.replaces:
                        # El total entre el `sub_total` y el `valor_remplazo` es >= 0, entonces "Remplazo OK":
                        if document.get_total_amount_with_replaces() >= 0:
                            return "Remplazo OK"
                        else:  # El total entre el `sub_total` y el `valor_remplazo` es < 0, entonces "Error Remplazo":
                            return "Error Remplazo"
                    else:  # Si en el juego de documentos no hay remplazos, entonces:
                        return "Error POS"
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
