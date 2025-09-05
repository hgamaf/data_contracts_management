"""
Schema definitions for data contracts management.
"""
from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class DataType(str, Enum):
    """Supported data types for schema fields."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    ARRAY = "array"
    OBJECT = "object"


class ValidationRule(BaseModel):
    """Validation rule for a schema field."""
    type: str = Field(..., description="Type of validation rule")
    value: Any = Field(..., description="Value or parameter for the rule")
    message: Optional[str] = Field(None, description="Custom error message")


class SchemaField(BaseModel):
    """Individual field definition in a schema."""
    name: str = Field(..., description="Field name")
    data_type: DataType = Field(..., description="Data type of the field")
    required: bool = Field(True, description="Whether the field is required")
    description: Optional[str] = Field(None, description="Field description")
    validation_rules: List[ValidationRule] = Field(default_factory=list, description="Validation rules")
    default_value: Optional[Any] = Field(None, description="Default value for the field")


class DataSchema(BaseModel):
    """Complete schema definition for a data contract."""
    name: str = Field(..., description="Schema name")
    version: str = Field("1.0.0", description="Schema version")
    description: Optional[str] = Field(None, description="Schema description")
    fields: List[SchemaField] = Field(..., description="List of schema fields")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    tags: List[str] = Field(default_factory=list, description="Schema tags")


class DataContract(BaseModel):
    """Data contract containing schema and metadata."""
    id: str = Field(..., description="Unique contract identifier")
    name: str = Field(..., description="Contract name")
    schema: DataSchema = Field(..., description="Data schema definition")
    owner: str = Field(..., description="Contract owner")
    status: Literal["draft", "active", "deprecated"] = Field("draft", description="Contract status")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


# Predefined common validation rules
COMMON_VALIDATION_RULES = {
    "min_length": lambda x: ValidationRule(type="min_length", value=x, message=f"Minimum length is {x}"),
    "max_length": lambda x: ValidationRule(type="max_length", value=x, message=f"Maximum length is {x}"),
    "min_value": lambda x: ValidationRule(type="min_value", value=x, message=f"Minimum value is {x}"),
    "max_value": lambda x: ValidationRule(type="max_value", value=x, message=f"Maximum value is {x}"),
    "pattern": lambda x: ValidationRule(type="pattern", value=x, message=f"Must match pattern: {x}"),
    "not_null": ValidationRule(type="not_null", value=True, message="Field cannot be null"),
    "unique": ValidationRule(type="unique", value=True, message="Field must be unique"),
}