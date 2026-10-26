from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

class StrategyIn(BaseModel):
    name: str
    description: Optional[str] = None
    enabled: bool = True

class StrategyConfig(BaseModel):
    strategy_id: int
    key: str
    schema_type: str  # "boolean","number","text","json"
    value_bool: Optional[bool] = None
    value_num: Optional[float] = None
    value_text: Optional[str] = None
    description: Optional[str] = None

    @validator("schema_type")
    def schema_type_allowed(cls, v):
        if v not in {"boolean", "number", "text", "json"}:
            raise ValueError("schema_type must be one of boolean|number|text|json")
        return v

class EMS_Status(BaseModel):
    flag_key: str
    flag_value: bool

class CycleConfig(BaseModel):
    strategy_id: int
    max_cycles_per_day: Optional[int] = 1
    cycle_min_soc: Optional[float] = None
    cycle_max_soc: Optional[float] = None
    min_rest_minutes: Optional[int] = 0

class SetpointWrite(BaseModel):
    strategy_id: Optional[int] = None
    timestamp: Optional[datetime] = None
    register_mode: Optional[int] = None
    setpoint_value: Optional[float] = None
    unit: Optional[str] = "kW"
    device_id: Optional[str] = None
    success: Optional[bool] = None
    response_text: Optional[str] = None

class ExecutionLog(BaseModel):
    strategy_id: Optional[int]
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    input_snapshot: Optional[Dict[str, Any]] = None
    decision_snapshot: Optional[Dict[str, Any]] = None
    error_text: Optional[str] = None

@dataclass
class Suggestion:
    id: Optional[int]
    ts: Optional[datetime]
    suggestion_type: str 
    priority: str
    title: str
    description: str
    metadata: Dict[str, Any]
    executed: bool = False
    executed_at: Optional[datetime] = None
    remarks: Optional[str] = None

class EnergyStrategy(BaseModel):
    ts: Optional[datetime] = None
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None
    type: Optional[str] = None
    tariff: Optional[float] = None
    power: Optional[float] = None
    duration: Optional[str] = None
    min_soc: Optional[float] = None
    max_soc: Optional[float] = None
    priority: Optional[str] = None
    remark: Optional[str] = None
    schedule_from: Optional[str] = None


class NotificationEvent(BaseModel):
    id: Optional[int] = None
    event_time: Optional[datetime] = None

    source: Optional[str] = None
    event_type: Optional[str] = None
    severity: Optional[str] = None
    title: Optional[str] = None
    message: Optional[str] = None

    entity_type: Optional[str] = None
    entity_id: Optional[str] = None

    status: Optional[str] = "active"
    acknowledged: Optional[bool] = False
    acknowledged_by: Optional[str] = None
    acknowledged_time: Optional[datetime] = None
    cleared_time: Optional[datetime] = None

    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
