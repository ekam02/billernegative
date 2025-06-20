from datetime import date
from typing import Any, List, Optional, Set

from sqlmodel import SQLModel


class Document(SQLModel):
    pass
    store_name: str
    doc_num: str
    doc_type: Optional[int] = None
    line: int
    store: int
    pos: int
    trx: int
    billed_at: date
    sent_at: Optional[date] = None
    amount: int
    customer: int
    duplicated: Optional[bool] = None
    status: Optional[str] = None
    log_dian: Optional[str] = None
    uuid: Optional[str] = None
    memos: Optional[List["Document"]] = None
    pair_up: Optional["Document"] = None
    replaces: Optional[Set["Document"]] = None
    evaluation: str = ""

    def get_memo_doc_nums(self) -> Optional[List[str]]:
        if not self.memos:
            return None
        return [memo.doc_num for memo in self.memos]

    def get_replace_doc_nums(self) -> Optional[List[str]]:
        if not self.replaces:
            return None
        return [replace.doc_num for replace in list(self.replaces)]

    def get_memo_total_amount(self) -> int:
        if not self.memos:
            return 0
        return sum([memo.amount for memo in self.memos])

    def get_replace_total_amount(self) -> int:
        if not self.replaces:
            return 0
        return sum([replace.amount for replace in list(self.replaces)])

    def get_amount_difference_with_memos(self) -> int:
        return self.amount - abs(self.get_memo_total_amount())

    def get_total_amount_with_replaces(self) -> int:
        return self.get_amount_difference_with_memos() + self.get_replace_total_amount()

    def __eq__(self, other) -> bool:
        if not isinstance(other, Document):
            return False
        return self.doc_num == other.doc_num and self.line == other.line and self.store == other.store and self.pos == other.pos and self.trx == other.trx and self.billed_at == other.billed_at and self.amount == other.amount and self.customer == other.customer

    def __hash__(self) -> int:
        return hash((self.doc_num, self.line, self.store, self.pos, self.trx, self.billed_at, self.amount, self.customer))
