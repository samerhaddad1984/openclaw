"""Oracles — ground-truth validators that score a runner's output."""
from .audit_oracle import AuditOracle
from .financial_oracle import FinancialOracle
from .receipt_oracle import ReceiptOracle
from .recon_oracle import ReconOracle
from .tax_oracle import TaxOracle
from .workflow_oracle import WorkflowOracle

ORACLES = {
    "receipt":   ReceiptOracle,
    "audit":     AuditOracle,
    "financial": FinancialOracle,
    "recon":     ReconOracle,
    "workflow":  WorkflowOracle,
    "tax":       TaxOracle,
}


def get_oracle(name: str):
    cls = ORACLES.get(name)
    if not cls:
        raise KeyError(f"no oracle named {name!r}")
    return cls()
