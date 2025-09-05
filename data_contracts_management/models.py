"""
Modelos de dados para contratos
"""
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class ContractStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    FAILED = "failed"


class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"


class FieldContract(BaseModel):
    """Contrato para um campo específico"""
    name: str
    data_type: DataType
    nullable: bool = False
    description: Optional[str] = None
    constraints: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "user_id",
                "data_type": "integer",
                "nullable": False,
                "description": "Identificador único do usuário",
                "constraints": {"min_value": 1}
            }
        }


class DataContract(BaseModel):
    """Contrato de dados principal"""
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    version: str = "1.0.0"
    status: ContractStatus = ContractStatus.DRAFT
    dataset_name: str
    fields: List[FieldContract]
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    owner: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "user_events_contract",
                "description": "Contrato para eventos de usuário",
                "version": "1.0.0",
                "dataset_name": "user_events",
                "fields": [
                    {
                        "name": "user_id",
                        "data_type": "integer",
                        "nullable": False,
                        "description": "ID do usuário"
                    }
                ],
                "owner": "data-team",
                "tags": ["events", "users"]
            }
        }


class ValidationResult(BaseModel):
    """Resultado de validação de contrato"""
    contract_id: str
    success: bool
    timestamp: datetime
    details: Dict[str, Any]
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class OpenMetadataConfig(BaseModel):
    """Configuração para integração com OpenMetadata"""
    server_url: str = "http://localhost:8585"
    auth_token: Optional[str] = None
    service_name: str = "data-contracts"
    enabled: bool = False