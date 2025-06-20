from datetime import date
from typing import Optional

from pandas import DataFrame, read_sql
from sqlalchemy import Engine, TextClause

from config import biller_engine, log_console_level, log_file_level
from config.logger_config import setup_logger
from models import BILLER_QUERY_NEGATIVE_INVOICES


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


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
        logger.error(f"The types provided are not correct. {e}")
    except Exception as e:
        print(f"An error occurred while retrieving negative invoices.: {e}")
    return None
