from config import start_date, end_date
from utils import create_documents, create_report, read_negative_invoices


if __name__ == "__main__":
    documents = read_negative_invoices(start_date, end_date)
    documents = create_documents(documents)
    create_report(documents)
