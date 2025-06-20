from datetime import date, timedelta
from typing import List, Optional

from pandas import DataFrame, read_sql
from sqlalchemy import Engine, TextClause
from sqlalchemy.orm import Session

from config import biller_engine, jano_engine, log_console_level, log_file_level
from config.logger_config import setup_logger
from models import BILLER_QUERY_NEGATIVE_INVOICES, JANO_QUERY_DOCUMENT_BY_ATTRIBUTES
from schemas import Document


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def create_documents(df: DataFrame) -> Optional[List[Document]]:
    try:
        if not isinstance(df, DataFrame):
            raise TypeError("The 'df' is expected to be of type 'DataFrame' from Pandas.")
        if df.empty:
            raise ValueError("The 'df' is empty.")
        return [Document(**row.to_dict()) for _, row in df.iterrows()]
    except TypeError as e:
        logger.exception(f"The types provided are not correct. {e}")
    except ValueError as e:
        logger.exception(f"An error occurred while creating documents: {e}")
    except Exception as e:
        logger.exception(f"An error occurred while creating documents: {e}")
    return None


def find_jano_document_by_attributes(
        line: int, store: int, pos: int, trx: int,
        billed_at: date,
        clause: TextClause = JANO_QUERY_DOCUMENT_BY_ATTRIBUTES,
        engine: Engine = jano_engine
) -> Optional[Document]:
    try:
        if not isinstance(line, int) or not isinstance(store, int) or not isinstance(pos, int) or not isinstance(trx, int):
            raise TypeError("It is expected that 'line', 'store', 'pos' and 'trx' are of type 'int'.")
        if not isinstance(billed_at, date):
            raise TypeError("It is expected that 'billed_at' is of type 'date'.")
        if not isinstance(clause, TextClause):
            raise TypeError("The 'clause' is expected to be of type 'TextClause' from SQLAlchemy.")
        if not isinstance(engine, Engine):
            raise TypeError("The 'engine' is expected to be of type 'Engine' from SQLAlchemy.")
        params = {
            "line": line, "store": store, "pos": pos, "trx": trx, "start_date": billed_at - timedelta(days=1),
            "end_date": billed_at + timedelta(days=1)
        }
        with Session(engine) as session:
            jano_documents = session.execute(clause, params).first()
            if jano_documents:
                return Document(**jano_documents.first()._asdict())
    except Exception as e:
        logger.exception(f"An error occurred while retrieving a Jano document: {e}")
    return None


def read_negative_invoices(
        start_date: date,
        end_date: date,
        clause: TextClause = BILLER_QUERY_NEGATIVE_INVOICES,
        engine: Engine = biller_engine
) -> Optional[DataFrame]:
    """Devuelve un DataFrame con todas las facturas negativas del periodo entregado"""
    try:
        if not isinstance(start_date, date) and not isinstance(end_date, date):
            raise TypeError("It is expected that 'start_date' and 'end_date' are of type 'date'.")
        if not isinstance(clause, TextClause):
            raise TypeError("The 'clause' is expected to be of type 'TextClause' from SQLAlchemy.")
        if not isinstance(engine, Engine):
            raise TypeError("The 'engine' is expected to be of type 'Engine' from SQLAlchemy.")

        params = {"start_date": start_date, "end_date": end_date}
        negative_invoices = read_sql(clause, engine, params=params)
        if not negative_invoices.empty:
            return negative_invoices
    except TypeError as e:
        logger.exception(f"The types provided are not correct. {e}")
    except Exception as e:
        logger.exception(f"An error occurred while retrieving negative invoices: {e}")
    return None
