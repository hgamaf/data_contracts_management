"""
Data validation using Pandera with comprehensive logging.
"""
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from typing import Dict, Any, List, Optional, Tuple
from loguru import logger
import sys
from datetime import datetime
import json

from schema import DataSchema, SchemaField, DataType, ValidationRule, DataContract


class DataValidator:
    """Validate data using Pandera schemas with detailed logging."""
    
    def __init__(self, log_level: str = "INFO"):
        """Initialize validator with logging configuration."""
        self.setup_logging(log_level)
        logger.info("DataValidator initialized")
    
    def setup_logging(self, log_level: str):
        """Configure loguru logging."""
        logger.remove()  # Remove default handler
        
        # Console logging
        logger.add(
            sys.stdout,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            colorize=True
        )
        
        # File logging
        logger.add(
            "data_contracts_management/logs/validation_{time:YYYY-MM-DD}.log",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="1 day",
            retention="30 days",
            compression="zip"
        )
    
    def convert_schema_to_pandera(self, data_schema: DataSchema) -> DataFrameSchema:
        """Convert our DataSchema to Pandera DataFrameSchema."""
        logger.info(f"Converting schema '{data_schema.name}' to Pandera format")
        
        columns = {}
        
        for field in data_schema.fields:
            logger.debug(f"Processing field: {field.name} ({field.data_type})")
            
            # Convert data type
            dtype = self._convert_data_type(field.data_type)
            
            # Convert validation rules to Pandera checks
            checks = self._convert_validation_rules(field.validation_rules)
            
            # Create column
            columns[field.name] = Column(
                dtype=dtype,
                checks=checks,
                nullable=not field.required,
                description=field.description
            )
            
            logger.debug(f"Created column for {field.name}: dtype={dtype}, nullable={not field.required}, checks={len(checks)}")
        
        schema = DataFrameSchema(
            columns=columns,
            name=data_schema.name,
            description=data_schema.description
        )
        
        logger.success(f"Successfully converted schema '{data_schema.name}' with {len(columns)} columns")
        return schema
    
    def _convert_data_type(self, data_type: DataType) -> Any:
        """Convert DataType enum to pandas/pandera dtype."""
        type_mapping = {
            DataType.STRING: "string",
            DataType.INTEGER: "int64",
            DataType.FLOAT: "float64",
            DataType.BOOLEAN: "bool",
            DataType.DATE: "string",  # Will validate format separately
            DataType.DATETIME: "string",  # Will validate format separately
            DataType.ARRAY: "object",
            DataType.OBJECT: "object"
        }
        
        return type_mapping.get(data_type, "object")
    
    def _convert_validation_rules(self, validation_rules: List[ValidationRule]) -> List[Check]:
        """Convert validation rules to Pandera checks."""
        checks = []
        
        for rule in validation_rules:
            logger.debug(f"Converting validation rule: {rule.type} = {rule.value}")
            
            if rule.type == "min_length":
                checks.append(Check.str_length(min_val=rule.value))
            elif rule.type == "max_length":
                checks.append(Check.str_length(max_val=rule.value))
            elif rule.type == "min_value":
                checks.append(Check.greater_than_or_equal_to(rule.value))
            elif rule.type == "max_value":
                checks.append(Check.less_than_or_equal_to(rule.value))
            elif rule.type == "pattern":
                checks.append(Check.str_matches(rule.value))
            elif rule.type == "not_null":
                checks.append(Check.notin([None, ""]))
            elif rule.type == "unique":
                # Note: Pandera doesn't have built-in unique check for columns
                # We'll handle this separately
                logger.warning(f"Unique constraint for field will be checked separately")
        
        return checks
    
    def validate_dataframe(self, df: pd.DataFrame, schema: DataFrameSchema, contract_id: str = None) -> Tuple[bool, Dict[str, Any]]:
        """Validate DataFrame against Pandera schema."""
        contract_info = f" (Contract: {contract_id})" if contract_id else ""
        logger.info(f"Starting validation for DataFrame with {len(df)} rows{contract_info}")
        
        validation_result = {
            "is_valid": False,
            "total_rows": len(df),
            "valid_rows": 0,
            "invalid_rows": 0,
            "errors": [],
            "warnings": [],
            "timestamp": datetime.now().isoformat(),
            "contract_id": contract_id,
            "schema_name": schema.name
        }
        
        try:
            # Validate schema
            validated_df = schema.validate(df, lazy=True)
            validation_result["is_valid"] = True
            validation_result["valid_rows"] = len(validated_df)
            
            logger.success(f"DataFrame validation passed! {len(validated_df)} rows validated successfully{contract_info}")
            
            # Check for unique constraints separately
            unique_violations = self._check_unique_constraints(df, schema)
            if unique_violations:
                validation_result["warnings"].extend(unique_violations)
                logger.warning(f"Found {len(unique_violations)} unique constraint violations")
            
        except pa.errors.SchemaErrors as e:
            validation_result["is_valid"] = False
            validation_result["invalid_rows"] = len(df)
            
            logger.error(f"Schema validation failed{contract_info}")
            
            # Process individual errors
            for error in e.schema_errors:
                error_info = {
                    "column": getattr(error, 'column', 'unknown'),
                    "check": str(getattr(error, 'check', 'unknown')),
                    "failure_cases": str(getattr(error, 'failure_cases', [])),
                    "message": str(error)
                }
                validation_result["errors"].append(error_info)
                
                logger.error(f"Validation error in column '{error_info['column']}': {error_info['message']}")
        
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append({
                "type": "unexpected_error",
                "message": str(e)
            })
            logger.exception(f"Unexpected error during validation{contract_info}")
        
        # Log summary
        self._log_validation_summary(validation_result)
        
        return validation_result["is_valid"], validation_result
    
    def _check_unique_constraints(self, df: pd.DataFrame, schema: DataFrameSchema) -> List[Dict[str, Any]]:
        """Check unique constraints separately."""
        violations = []
        
        for column_name, column_schema in schema.columns.items():
            if column_name in df.columns:
                # Check if any validation rules indicate uniqueness
                # This is a simplified check - in practice you'd store this info differently
                duplicates = df[df[column_name].duplicated()]
                if not duplicates.empty:
                    violations.append({
                        "type": "unique_violation",
                        "column": column_name,
                        "duplicate_count": len(duplicates),
                        "message": f"Found {len(duplicates)} duplicate values in column '{column_name}'"
                    })
        
        return violations
    
    def _log_validation_summary(self, result: Dict[str, Any]):
        """Log validation summary."""
        contract_info = f" for contract {result['contract_id']}" if result['contract_id'] else ""
        
        if result["is_valid"]:
            logger.info(f"✅ Validation Summary{contract_info}:")
            logger.info(f"   Total rows: {result['total_rows']}")
            logger.info(f"   Valid rows: {result['valid_rows']}")
            if result["warnings"]:
                logger.info(f"   Warnings: {len(result['warnings'])}")
        else:
            logger.error(f"❌ Validation Summary{contract_info}:")
            logger.error(f"   Total rows: {result['total_rows']}")
            logger.error(f"   Invalid rows: {result['invalid_rows']}")
            logger.error(f"   Errors: {len(result['errors'])}")
    
    def validate_contract_data(self, df: pd.DataFrame, contract: DataContract) -> Tuple[bool, Dict[str, Any]]:
        """Validate DataFrame against a DataContract."""
        logger.info(f"Validating data against contract: {contract.name} (ID: {contract.id})")
        
        # Convert schema to Pandera
        pandera_schema = self.convert_schema_to_pandera(contract.schema)
        
        # Validate
        is_valid, result = self.validate_dataframe(df, pandera_schema, contract.id)
        
        # Add contract metadata to result
        result["contract_name"] = contract.name
        result["contract_owner"] = contract.owner
        result["contract_status"] = contract.status
        result["schema_version"] = contract.schema.version
        
        return is_valid, result
    
    def save_validation_report(self, result: Dict[str, Any], filename: Optional[str] = None) -> str:
        """Save validation result to JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            schema_name = result.get('schema_name', 'unknown')
            filename = f"data_contracts_management/logs/validation_report_{schema_name}_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Validation report saved to: {filename}")
        return filename


def create_logs_directory():
    """Create logs directory if it doesn't exist."""
    import os
    os.makedirs("data_contracts_management/logs", exist_ok=True)


# Initialize logs directory when module is imported
create_logs_directory()