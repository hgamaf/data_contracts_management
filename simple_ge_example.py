"""
Simplified Great Expectations example that works with current API.
"""
import great_expectations as gx
import pandas as pd
from loguru import logger
from data.generator import DataGenerator, create_customer_schema
from schema import DataContract
import os


def simple_great_expectations_demo():
    """Simple demo that creates and validates data with Great Expectations."""
    logger.info("🚀 Starting simple Great Expectations demo")
    
    # Initialize Great Expectations context
    logger.info("📋 Setting up Great Expectations...")
    
    # Create context in current directory
    context = gx.get_context()
    
    # Generate sample data
    logger.info("🎲 Generating sample data...")
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    
    # Generate data
    data = generator.generate_dataset(customer_schema, num_records=50)
    df = pd.DataFrame(data)
    
    logger.info(f"Generated {len(df)} customer records")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Create a simple expectation suite manually
    logger.info("📝 Creating expectation suite...")
    
    try:
        # Try to create a simple suite
        suite = context.suites.add()
        suite_name = suite.name
        logger.info(f"Created suite: {suite_name}")
        
        # Add some basic expectations
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="customer_id")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="email")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="first_name")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="last_name")
        )
        
        logger.success(f"Added expectations to suite: {suite_name}")
        
        # Create a data source and asset
        logger.info("📊 Setting up data source...")
        
        # Add pandas datasource
        datasource = context.data_sources.add_pandas("pandas_datasource")
        
        # Add dataframe asset
        asset = datasource.add_dataframe_asset("customer_data")
        
        # Create batch request
        batch_request = asset.build_batch_request(dataframe=df)
        
        # Validate the data
        logger.info("🔍 Validating data...")
        
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        # Run validation
        validation_result = validator.validate()
        
        # Process results
        logger.info("📈 Validation Results:")
        logger.info(f"   Success: {validation_result.success}")
        logger.info(f"   Total expectations: {len(validation_result.results)}")
        
        successful = sum(1 for r in validation_result.results if r.success)
        failed = len(validation_result.results) - successful
        
        logger.info(f"   Passed: {successful}")
        logger.info(f"   Failed: {failed}")
        
        if validation_result.success:
            logger.success("✅ All validations passed!")
        else:
            logger.warning("⚠️ Some validations failed:")
            for result in validation_result.results:
                if not result.success:
                    logger.warning(f"   - {result.expectation_config.expectation_type}")
        
        # Build data docs
        logger.info("📊 Building data docs...")
        context.build_data_docs()
        
        # Get data docs URL
        try:
            docs_sites = context.get_docs_sites_urls()
            if docs_sites:
                for site in docs_sites:
                    url = site.get('site_url', '')
                    if url:
                        logger.success(f"🌐 Data docs available at: {url}")
                        logger.info("Open this URL in your browser to view the validation results!")
        except Exception as e:
            logger.warning(f"Could not get docs URL: {e}")
            logger.info("Check the gx/uncommitted/data_docs/ directory for HTML files")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Error in Great Expectations demo: {e}")
        logger.exception("Full error details:")
        return None


def test_with_invalid_data():
    """Test validation with intentionally invalid data."""
    logger.info("🧪 Testing with invalid data...")
    
    context = gx.get_context()
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    
    # Generate data with issues
    data = generator.generate_dataset(customer_schema, num_records=20)
    
    # Introduce problems
    for i in range(5):
        data[i]['customer_id'] = None  # Null required field
        data[i]['first_name'] = ""     # Empty required field
        data[i]['email'] = "invalid"   # Invalid email
    
    df = pd.DataFrame(data)
    
    logger.info(f"Created dataset with {len(df)} records (some invalid)")
    
    try:
        # Get existing suite or create new one
        suites = context.suites.all()
        if suites:
            suite = suites[0]  # Use first available suite
            logger.info(f"Using existing suite: {suite.name}")
        else:
            logger.warning("No existing suites found, creating new one...")
            suite = context.suites.add()
            # Add basic expectations
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
            )
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column="first_name")
            )
        
        # Get datasource
        datasources = context.data_sources.all()
        if datasources:
            datasource = datasources[0]
            assets = datasource.assets.all()
            if assets:
                asset = assets[0]
            else:
                asset = datasource.add_dataframe_asset("invalid_test_data")
        else:
            datasource = context.data_sources.add_pandas("test_datasource")
            asset = datasource.add_dataframe_asset("invalid_test_data")
        
        # Validate
        batch_request = asset.build_batch_request(dataframe=df)
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        validation_result = validator.validate()
        
        logger.info("📈 Invalid Data Test Results:")
        logger.info(f"   Success: {validation_result.success}")
        
        if not validation_result.success:
            logger.success("✅ Invalid data correctly detected!")
            failed_expectations = [r for r in validation_result.results if not r.success]
            logger.info(f"   Failed expectations: {len(failed_expectations)}")
        else:
            logger.warning("⚠️ Invalid data was not detected - check expectations")
        
        # Update data docs
        context.build_data_docs()
        
    except Exception as e:
        logger.error(f"Error in invalid data test: {e}")


if __name__ == "__main__":
    # Run simple demo
    result = simple_great_expectations_demo()
    
    if result:
        print("\n" + "="*50 + "\n")
        # Test with invalid data
        test_with_invalid_data()
        
        logger.success("🎉 Great Expectations demo completed!")
        logger.info("💡 Next steps:")
        logger.info("   - Open the data docs URL to explore the UI")
        logger.info("   - Add more sophisticated expectations")
        logger.info("   - Set up automated validation pipelines")
    else:
        logger.error("❌ Demo failed - check the logs above")