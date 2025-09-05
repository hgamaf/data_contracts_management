"""
Great Expectations integration for data validation with UI.
"""
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
import pandas as pd
from typing import Dict, Any, List, Optional
from loguru import logger
import json
from datetime import datetime
import os

from schema import DataSchema, SchemaField, DataType, DataContract, ValidationRule


class GreatExpectationsValidator:
    """Great Expectations validator with UI integration."""
    
    def __init__(self, project_root: str = "data_contracts_management"):
        """Initialize Great Expectations context."""
        self.project_root = project_root
        self.ge_dir = os.path.join(project_root, "gx")
        self.context = None
        self.setup_context()
        
    def setup_context(self):
        """Setup Great Expectations context and configuration."""
        logger.info("Setting up Great Expectations context...")
        
        try:
            # Try to get existing context
            self.context = gx.get_context(context_root_dir=self.ge_dir)
            logger.info("Found existing Great Expectations context")
        except Exception:
            # Create new context
            logger.info("Creating new Great Expectations context...")
            self.context = gx.get_context(context_root_dir=self.ge_dir, mode="file")
            
            # Configure data source
            self._setup_datasource()
            
        logger.success("Great Expectations context ready!")
    
    def _setup_datasource(self):
        """Setup pandas datasource for runtime data."""
        logger.info("Setting up pandas datasource...")
        
        datasource_config = {
            "name": "pandas_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    "batch_identifiers": ["batch_id"],
                },
            },
        }
        
        try:
            self.context.add_datasource(**datasource_config)
            logger.success("Pandas datasource configured")
        except Exception as e:
            logger.warning(f"Datasource might already exist: {e}")
    
    def convert_schema_to_expectations(self, data_schema: DataSchema) -> List[Dict[str, Any]]:
        """Convert DataSchema to Great Expectations expectations."""
        logger.info(f"Converting schema '{data_schema.name}' to Great Expectations format")
        
        expectations = []
        
        # Basic table expectation
        expectations.append({
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {
                "column_list": [field.name for field in data_schema.fields]
            }
        })
        
        # Field-specific expectations
        for field in data_schema.fields:
            logger.debug(f"Processing field: {field.name}")
            
            # Required field expectation
            if field.required:
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": field.name}
                })
            
            # Data type expectations
            if field.data_type == DataType.STRING:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": field.name, "type_": "str"}
                })
            elif field.data_type == DataType.INTEGER:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": field.name, "type_": "int"}
                })
            elif field.data_type == DataType.FLOAT:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": field.name, "type_": "float"}
                })
            elif field.data_type == DataType.BOOLEAN:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": field.name, "type_": "bool"}
                })
            
            # Validation rules expectations
            for rule in field.validation_rules:
                expectation = self._convert_validation_rule(field.name, rule)
                if expectation:
                    expectations.append(expectation)
        
        logger.success(f"Created {len(expectations)} expectations for schema '{data_schema.name}'")
        return expectations
    
    def _convert_validation_rule(self, column_name: str, rule: ValidationRule) -> Optional[Dict[str, Any]]:
        """Convert validation rule to Great Expectations expectation."""
        if rule.type == "min_length":
            return {
                "expectation_type": "expect_column_value_lengths_to_be_between",
                "kwargs": {"column": column_name, "min_value": rule.value}
            }
        elif rule.type == "max_length":
            return {
                "expectation_type": "expect_column_value_lengths_to_be_between",
                "kwargs": {"column": column_name, "max_value": rule.value}
            }
        elif rule.type == "min_value":
            return {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": column_name, "min_value": rule.value}
            }
        elif rule.type == "max_value":
            return {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": column_name, "max_value": rule.value}
            }
        elif rule.type == "pattern":
            return {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {"column": column_name, "regex": rule.value}
            }
        elif rule.type == "unique":
            return {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": column_name}
            }
        
        return None
    
    def create_expectation_suite(self, data_schema: DataSchema, suite_name: Optional[str] = None) -> str:
        """Create expectation suite from DataSchema."""
        if suite_name is None:
            suite_name = f"{data_schema.name}_suite"
        
        logger.info(f"Creating expectation suite: {suite_name}")
        
        # Create suite
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        # Add expectations
        expectations = self.convert_schema_to_expectations(data_schema)
        
        for expectation_config in expectations:
            suite.add_expectation(expectation_configuration=expectation_config)
        
        # Save suite
        self.context.save_expectation_suite(suite)
        
        logger.success(f"Expectation suite '{suite_name}' created with {len(expectations)} expectations")
        return suite_name
    
    def validate_dataframe(self, df: pd.DataFrame, suite_name: str, batch_id: str = None) -> Dict[str, Any]:
        """Validate DataFrame using Great Expectations suite."""
        if batch_id is None:
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Validating DataFrame with suite '{suite_name}' (batch: {batch_id})")
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="runtime_data_asset",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": batch_id},
        )
        
        # Get validator
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )
        
        # Run validation
        validation_result = validator.validate()
        
        # Process results
        result_summary = {
            "success": validation_result.success,
            "suite_name": suite_name,
            "batch_id": batch_id,
            "total_expectations": len(validation_result.results),
            "successful_expectations": sum(1 for r in validation_result.results if r.success),
            "failed_expectations": sum(1 for r in validation_result.results if not r.success),
            "validation_time": validation_result.meta.get("validation_time"),
            "results": []
        }
        
        # Add detailed results
        for result in validation_result.results:
            result_detail = {
                "expectation_type": result.expectation_config.expectation_type,
                "success": result.success,
                "kwargs": result.expectation_config.kwargs,
            }
            
            if not result.success:
                result_detail["result"] = result.result
            
            result_summary["results"].append(result_detail)
        
        # Log summary
        if result_summary["success"]:
            logger.success(f"✅ Validation passed! {result_summary['successful_expectations']}/{result_summary['total_expectations']} expectations met")
        else:
            logger.error(f"❌ Validation failed! {result_summary['failed_expectations']}/{result_summary['total_expectations']} expectations failed")
        
        return result_summary
    
    def validate_contract_data(self, df: pd.DataFrame, contract: DataContract) -> Dict[str, Any]:
        """Validate DataFrame against DataContract using Great Expectations."""
        logger.info(f"Validating data against contract: {contract.name}")
        
        # Create or get expectation suite
        suite_name = f"{contract.id}_suite"
        
        try:
            # Try to get existing suite
            self.context.get_expectation_suite(suite_name)
            logger.info(f"Using existing expectation suite: {suite_name}")
        except Exception:
            # Create new suite
            logger.info(f"Creating new expectation suite for contract: {contract.name}")
            self.create_expectation_suite(contract.data_schema, suite_name)
        
        # Validate data
        result = self.validate_dataframe(df, suite_name, f"{contract.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        # Add contract metadata
        result["contract_id"] = contract.id
        result["contract_name"] = contract.name
        result["contract_owner"] = contract.owner
        result["schema_version"] = contract.data_schema.version
        
        return result
    
    def create_checkpoint(self, suite_name: str, checkpoint_name: Optional[str] = None) -> str:
        """Create checkpoint for automated validation."""
        if checkpoint_name is None:
            checkpoint_name = f"{suite_name}_checkpoint"
        
        logger.info(f"Creating checkpoint: {checkpoint_name}")
        
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "template_name": None,
            "module_name": "great_expectations.checkpoint",
            "class_name": "SimpleCheckpoint",
            "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
            "expectation_suite_name": suite_name,
            "batch_request": {},
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                    },
                },
                {
                    "name": "update_data_docs",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                    },
                },
            ],
        }
        
        self.context.add_checkpoint(**checkpoint_config)
        logger.success(f"Checkpoint '{checkpoint_name}' created")
        
        return checkpoint_name
    
    def build_data_docs(self):
        """Build and update data docs for UI."""
        logger.info("Building Great Expectations data docs...")
        
        try:
            self.context.build_data_docs()
            logger.success("✅ Data docs built successfully!")
            
            # Get docs URL
            docs_sites = self.context.get_docs_sites_urls()
            if docs_sites:
                for site_name, url in docs_sites.items():
                    logger.info(f"📊 Data docs available at: {url}")
            
        except Exception as e:
            logger.error(f"Failed to build data docs: {e}")
    
    def get_validation_results(self, suite_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get validation results from store."""
        logger.info("Retrieving validation results...")
        
        try:
            validation_results = []
            
            # Get all validation results
            results_store = self.context.stores["validations_store"]
            
            for key in results_store.list_keys():
                if suite_name is None or suite_name in str(key):
                    result = results_store.get(key)
                    validation_results.append({
                        "run_id": str(key.run_id),
                        "suite_name": key.expectation_suite_identifier.expectation_suite_name,
                        "success": result.success,
                        "timestamp": result.meta.get("validation_time"),
                        "statistics": result.statistics
                    })
            
            logger.info(f"Retrieved {len(validation_results)} validation results")
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to retrieve validation results: {e}")
            return []
    
    def start_ui_server(self, port: int = 8080):
        """Start Great Expectations UI server."""
        logger.info(f"Starting Great Expectations UI server on port {port}...")
        
        try:
            # Build docs first
            self.build_data_docs()
            
            # The UI is served through the built data docs
            docs_sites = self.context.get_docs_sites_urls()
            if docs_sites:
                for site_name, url in docs_sites.items():
                    logger.success(f"🚀 Great Expectations UI available at: {url}")
                    logger.info("Open this URL in your browser to view the validation results and data docs")
            else:
                logger.warning("No data docs sites configured")
                
        except Exception as e:
            logger.error(f"Failed to start UI server: {e}")


def setup_great_expectations_project():
    """Setup Great Expectations project structure."""
    logger.info("Setting up Great Expectations project...")
    
    # Create GE validator
    ge_validator = GreatExpectationsValidator()
    
    # Build initial data docs
    ge_validator.build_data_docs()
    
    logger.success("Great Expectations project setup complete!")
    return ge_validator