from config import start_date, end_date, log_console_level, log_file_level
from config.logger_config import setup_logger
from utils import create_documents, create_report, read_negative_invoices


logger = setup_logger(__name__, console_level=log_console_level, file_level=log_file_level)


def main():
    documents = read_negative_invoices(start_date, end_date)
    logger.info(f"Documents retrieved successfully: {len(documents)}")
    documents = create_documents(documents)
    logger.info(f"Documents created successfully: {len(documents)}")
    create_report(documents)
    logger.info("Report created successfully.")


if __name__ == "__main__":
    main()
