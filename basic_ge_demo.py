"""
Basic Great Expectations demo using core functionality.
"""
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
from loguru import logger
from data.generator import DataGenerator, create_customer_schema
import json


def basic_ge_demo():
    """Basic Great Expectations demo."""
    logger.info("🚀 Starting basic Great Expectations demo")
    
    # Get context
    context = gx.get_context()
    logger.info("✅ Great Expectations context ready")
    
    # Generate sample data
    logger.info("🎲 Generating sample data...")
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    
    data = generator.generate_dataset(customer_schema, num_records=100)
    df = pd.DataFrame(data)
    
    logger.info(f"Generated {len(df)} records with columns: {list(df.columns)}")
    
    # Create expectation suite manually
    logger.info("📝 Creating expectation suite...")
    
    suite_name = "customer_validation_suite"
    suite = ExpectationSuite(name=suite_name)
    
    # Add expectations manually
    expectations = [
        {
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {
                "column_list": ["customer_id", "first_name", "last_name", "email", "phone", "birth_date", "created_at", "is_active"]
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "customer_id"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null", 
            "kwargs": {"column": "first_name"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "last_name"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "email"}
        },
        {
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {"column": "customer_id", "type_": "int"}
        },
        {
            "expectation_type": "expect_column_values_to_be_of_type",
            "kwargs": {"column": "is_active", "type_": "bool"}
        }
    ]
    
    for exp_config in expectations:
        suite.add_expectation(exp_config)
    
    logger.success(f"Created suite '{suite_name}' with {len(expectations)} expectations")
    
    # Save the suite
    try:
        context.save_expectation_suite(suite)
        logger.success("✅ Expectation suite saved")
    except Exception as e:
        logger.warning(f"Could not save suite: {e}")
    
    # Create batch request for validation
    logger.info("🔍 Validating data...")
    
    try:
        # Create runtime batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector", 
            data_asset_name="customer_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "customer_batch"}
        )
        
        # Get validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        # Run validation
        validation_result = validator.validate()
        
        # Process results
        logger.info("📊 Validation Results:")
        logger.info(f"   Overall success: {validation_result.success}")
        logger.info(f"   Total expectations: {len(validation_result.results)}")
        
        passed = sum(1 for r in validation_result.results if r.success)
        failed = len(validation_result.results) - passed
        
        logger.info(f"   Passed: {passed}")
        logger.info(f"   Failed: {failed}")
        
        # Show detailed results
        for i, result in enumerate(validation_result.results):
            expectation_type = result.expectation_config.expectation_type
            status = "✅ PASS" if result.success else "❌ FAIL"
            logger.info(f"   {i+1}. {expectation_type}: {status}")
            
            if not result.success and hasattr(result, 'result'):
                logger.warning(f"      Details: {result.result}")
        
        # Build data docs
        logger.info("📊 Building data docs...")
        try:
            context.build_data_docs()
            
            # Try to get docs URL
            docs_sites = context.get_docs_sites_urls()
            if docs_sites:
                for site in docs_sites:
                    if isinstance(site, dict) and 'site_url' in site:
                        logger.success(f"🌐 Data docs: {site['site_url']}")
                    else:
                        logger.info(f"📊 Data docs site: {site}")
            
            logger.success("✅ Data docs built successfully!")
            
        except Exception as e:
            logger.warning(f"Could not build data docs: {e}")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        logger.exception("Full error:")
        return None


def test_invalid_data():
    """Test with invalid data to see failures."""
    logger.info("🧪 Testing with invalid data...")
    
    context = gx.get_context()
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    
    # Generate data and introduce errors
    data = generator.generate_dataset(customer_schema, num_records=20)
    
    # Make some data invalid
    data[0]['customer_id'] = None
    data[1]['first_name'] = None
    data[2]['email'] = None
    data[3]['customer_id'] = "invalid_id"  # Wrong type
    
    df = pd.DataFrame(data)
    logger.info(f"Created test dataset with {len(df)} records (some invalid)")
    
    # Use existing suite
    try:
        suite = context.get_expectation_suite("customer_validation_suite")
        logger.info("Using existing expectation suite")
    except Exception:
        logger.warning("Could not get existing suite, creating basic one...")
        suite = ExpectationSuite(name="test_suite")
        suite.add_expectation({
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "customer_id"}
        })
        suite.add_expectation({
            "expectation_type": "expect_column_values_to_not_be_null", 
            "kwargs": {"column": "first_name"}
        })
    
    # Validate
    try:
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="invalid_test_data", 
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "invalid_batch"}
        )
        
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        validation_result = validator.validate()
        
        logger.info("📊 Invalid Data Test Results:")
        logger.info(f"   Overall success: {validation_result.success}")
        
        if not validation_result.success:
            logger.success("✅ Invalid data correctly detected!")
            failed_expectations = [r for r in validation_result.results if not r.success]
            logger.info(f"   Failed expectations: {len(failed_expectations)}")
            
            for result in failed_expectations:
                expectation_type = result.expectation_config.expectation_type
                logger.warning(f"   - {expectation_type}")
        else:
            logger.warning("⚠️ All validations passed - invalid data not detected")
        
        # Update data docs
        context.build_data_docs()
        
    except Exception as e:
        logger.error(f"Invalid data test failed: {e}")


def show_data_summary(df):
    """Show summary of generated data."""
    logger.info("📋 Data Summary:")
    logger.info(f"   Shape: {df.shape}")
    logger.info(f"   Columns: {list(df.columns)}")
    
    # Show data types
    logger.info("   Data types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"     {col}: {dtype}")
    
    # Show sample data
    logger.info("   Sample records:")
    for i in range(min(3, len(df))):
        record = df.iloc[i].to_dict()
        logger.info(f"     Record {i+1}: {json.dumps(record, default=str, ensure_ascii=False)[:100]}...")


if __name__ == "__main__":
    # Generate and show data
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    data = generator.generate_dataset(customer_schema, num_records=10)
    df = pd.DataFrame(data)
    
    show_data_summary(df)
    
    print("\n" + "="*60 + "\n")
    
    # Run validation demo
    result = basic_ge_demo()
    
    if result:
        print("\n" + "="*60 + "\n")
        test_invalid_data()
        
        logger.success("🎉 Great Expectations demo completed!")
        logger.info("💡 Check the data docs URL above to explore the UI")
    else:
        logger.error("❌ Demo failed")