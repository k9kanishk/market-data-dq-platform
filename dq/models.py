from __future__ import annotations
from datetime import datetime, date
from typing import Optional
from sqlmodel import SQLModel, Field, Column, JSON

class RiskFactor(SQLModel, table=True):
    id: str = Field(primary_key=True)
    asset_class: str = Field(index=True)
    description: str
    unit: str

class DataSource(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)       # fred, yfinance, ecb_fx, stooq
    symbol: str = Field(index=True)
    field: str = "value"
    meta: dict = Field(default_factory=dict, sa_column=Column(JSON))

class Observation(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    risk_factor_id: str = Field(foreign_key="riskfactor.id", index=True)
    source_id: int = Field(foreign_key="datasource.id", index=True)
    obs_date: date = Field(index=True)
    value: float
    ingested_at: datetime = Field(default_factory=datetime.utcnow, index=True)

class DQRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    asset_class: str = Field(index=True)
    risk_factor_id: str = Field(index=True)
    asof: date = Field(index=True)
    started_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    finished_at: Optional[datetime] = Field(default=None, index=True)
    parameters: dict = Field(default_factory=dict, sa_column=Column(JSON))

class DQException(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dq_run_id: int = Field(foreign_key="dqrun.id", index=True)
    risk_factor_id: str = Field(index=True)
    rule: str = Field(index=True)
    obs_date: date = Field(index=True)
    severity: int = Field(index=True)  # 1..100
    status: str = Field(default="open", index=True)  # open|triaged|closed
    suggested_action: str = Field(default="review", index=True)
    details: dict = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

class ExceptionAction(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    exception_id: int = Field(foreign_key="dqexception.id", index=True)
    action: str
    comment: str = ""
    actor: str = "analyst"
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
