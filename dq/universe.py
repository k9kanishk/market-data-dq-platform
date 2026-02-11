from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import yaml

@dataclass(frozen=True)
class SourceSpec:
    name: str
    symbol: str
    field: str = "value"
    meta: Dict[str, Any] | None = None

@dataclass(frozen=True)
class RiskFactorSpec:
    id: str
    asset_class: str
    description: str
    unit: str
    sources: List[SourceSpec]

def load_universe(path: str | Path) -> List[RiskFactorSpec]:
    p = Path(path)
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    out: List[RiskFactorSpec] = []
    for rf in doc["risk_factors"]:
        sources = []
        for s in rf.get("sources", []):
            meta = {k: v for k, v in s.items() if k not in {"name", "symbol", "field"}}
            sources.append(SourceSpec(s["name"], s["symbol"], s.get("field", "value"), meta or None))
        out.append(RiskFactorSpec(rf["id"], rf["asset_class"], rf["description"], rf["unit"], sources))
    return out
