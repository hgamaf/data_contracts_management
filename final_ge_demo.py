"""
Final working Great Expectations demo.
"""
import great_expectations as gx
import pandas as pd
from loguru import logger
from data.generator import DataGenerator, create_customer_schema
import webbrowser
import os


def working_ge_demo():
    """Working Great Expectations demo with UI."""
    logger.info("🚀 Starting Great Expectations demo")
    
    # Initialize context
    context = gx.get_context()
    logger.success("✅ Great Expectations context ready")
    
    # Generate sample data
    logger.info("🎲 Generating sample data...")
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    
    data = generator.generate_dataset(customer_schema, num_records=100)
    df = pd.DataFrame(data)
    
    logger.info(f"Generated {len(df)} customer records")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Show sample data
    logger.info("📋 Sample data:")
    for i in range(3):
        record = df.iloc[i]
        logger.info(f"   Customer {i+1}: ID={record['customer_id']}, Name={record['first_name']} {record['last_name']}, Email={record['email']}")
    
    # Create data source and asset
    logger.info("📊 Setting up data source...")
    
    try:
        # Add pandas datasource
        datasource = context.data_sources.add_pandas("customer_datasource")
        logger.success("✅ Created pandas datasource")
    except Exception as e:
        logger.warning(f"Datasource might exist: {e}")
        datasource = context.data_sources.get("customer_datasource")
    
    try:
        # Add dataframe asset
        asset = datasource.add_dataframe_asset("customer_data")
        logger.success("✅ Created dataframe asset")
    except Exception as e:
        logger.warning(f"Asset might exist: {e}")
        asset = datasource.get_asset("customer_data")
    
    # Create batch request
    batch_request = asset.build_batch_request(options={"dataframe": df})
    
    # Create expectation suite using the fluent API
    logger.info("📝 Creating expectations...")
    
    try:
        # Create validator with batch request
        validator = context.get_validator(batch_request=batch_request)
        
        # Add expectations using the validator
        logger.info("Adding expectations...")
        
        # Basic table structure expectations
        validator.expect_table_row_count_to_be_between(min_value=50, max_value=200)
        validator.expect_table_column_count_to_equal(8)
        
        # Column existence expectations
        for col in df.columns:
            validator.expect_column_to_exist(col)
        
        # Data type and null expectations
        validator.expect_column_values_to_not_be_null("customer_id")
        validator.expect_column_values_to_not_be_null("first_name")
        validator.expect_column_values_to_not_be_null("last_name")
        validator.expect_column_values_to_not_be_null("email")
        validator.expect_column_values_to_not_be_null("created_at")
        validator.expect_column_values_to_not_be_null("is_active")
        
        # Type expectations
        validator.expect_column_values_to_be_of_type("customer_id", "int64")
        validator.expect_column_values_to_be_of_type("is_active", "bool")
        
        # Value expectations
        validator.expect_column_values_to_be_between("customer_id", min_value=0, max_value=1000000)
        validator.expect_column_values_to_be_in_set("is_active", [True, False])
        
        # String length expectations
        validator.expect_column_value_lengths_to_be_between("first_name", min_value=2, max_value=50)
        validator.expect_column_value_lengths_to_be_between("last_name", min_value=2, max_value=50)
        
        # Email format expectation (basic)
        validator.expect_column_values_to_match_regex("email", r"^[^@]+@[^@]+\.[^@]+$")
        
        logger.success("✅ Added expectations to validator")
        
        # Save the expectation suite
        suite = validator.expectation_suite
        suite_name = f"customer_suite_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
        suite.name = suite_name
        
        context.suites.add(suite)
        logger.success(f"✅ Saved expectation suite: {suite_name}")
        
        # Run validation
        logger.info("🔍 Running validation...")
        validation_result = validator.validate()
        
        # Process results
        logger.info("📊 Validation Results:")
        logger.info(f"   Overall success: {validation_result.success}")
        logger.info(f"   Total expectations: {len(validation_result.results)}")
        
        passed = sum(1 for r in validation_result.results if r.success)
        failed = len(validation_result.results) - passed
        
        logger.info(f"   ✅ Passed: {passed}")
        logger.info(f"   ❌ Failed: {failed}")
        
        if validation_result.success:
            logger.success("🎉 All validations passed!")
        else:
            logger.warning("⚠️ Some validations failed:")
            for result in validation_result.results:
                if not result.success:
                    expectation_type = result.expectation_config.expectation_type
                    logger.warning(f"   - {expectation_type}")
        
        # Build data docs
        logger.info("📊 Building data docs...")
        context.build_data_docs()
        
        # Get and open data docs URL
        try:
            docs_sites = context.get_docs_sites_urls()
            if docs_sites:
                for site in docs_sites:
                    if isinstance(site, dict) and 'site_url' in site:
                        url = site['site_url']
                        logger.success(f"🌐 Data docs available at: {url}")
                        
                        # Try to open in browser
                        try:
                            webbrowser.open(url)
                            logger.info("🚀 Opened data docs in browser!")
                        except Exception:
                            logger.info("💡 Copy the URL above and open it in your browser")
                        
                        return url
        except Exception as e:
            logger.warning(f"Could not get docs URL: {e}")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Error in validation: {e}")
        logger.exception("Full error:")
        return None


def test_with_bad_data():
    """Test validation with intentionally bad data."""
    logger.info("🧪 Testing with invalid data...")
    
    context = gx.get_context()
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    
    # Generate data and make it bad
    data = generator.generate_dataset(customer_schema, num_records=30)
    
    # Introduce various problems
    data[0]['customer_id'] = None  # Null required field
    data[1]['first_name'] = ""     # Empty string
    data[2]['email'] = "invalid"   # Invalid email
    data[3]['customer_id'] = -1    # Negative ID
    data[4]['is_active'] = "yes"   # Wrong type
    
    df = pd.DataFrame(data)
    logger.info(f"Created test dataset with {len(df)} records (some invalid)")
    
    try:
        # Get existing datasource and asset
        datasource = context.data_sources.get("customer_datasource")
        asset = datasource.get_asset("customer_data")
        
        # Create batch request
        batch_request = asset.build_batch_request(options={"dataframe": df})
        
        # Get existing suite
        suites = context.suites.all()
        if suites:
            suite = suites[-1]  # Use most recent suite
            logger.info(f"Using existing suite: {suite.name}")
            
            # Validate with existing suite
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite=suite
            )
            
            validation_result = validator.validate()
            
            logger.info("📊 Invalid Data Test Results:")
            logger.info(f"   Overall success: {validation_result.success}")
            
            passed = sum(1 for r in validation_result.results if r.success)
            failed = len(validation_result.results) - passed
            
            logger.info(f"   ✅ Passed: {passed}")
            logger.info(f"   ❌ Failed: {failed}")
            
            if not validation_result.success:
                logger.success("✅ Invalid data correctly detected!")
                
                # Show some failed expectations
                failed_expectations = [r for r in validation_result.results if not r.success]
                logger.info("   Failed expectations:")
                for result in failed_expectations[:5]:  # Show first 5
                    expectation_type = result.expectation_config.expectation_type
                    logger.warning(f"     - {expectation_type}")
            else:
                logger.warning("⚠️ All validations passed - invalid data not detected")
            
            # Update data docs
            context.build_data_docs()
            logger.success("✅ Updated data docs with new validation results")
            
        else:
            logger.warning("No existing expectation suites found")
            
    except Exception as e:
        logger.error(f"Invalid data test failed: {e}")


if __name__ == "__main__":
    # Run main demo
    result = working_ge_demo()
    
    if result:
        print("\n" + "="*60 + "\n")
        
        # Test with invalid data
        test_with_bad_data()
        
        logger.success("🎉 Great Expectations demo completed!")
        logger.info("💡 Key features demonstrated:")
        logger.info("   ✅ Data validation with comprehensive expectations")
        logger.info("   ✅ Automatic data docs generation")
        logger.info("   ✅ Web UI for viewing validation results")
        logger.info("   ✅ Detection of data quality issues")
        logger.info("   ✅ Integration with pandas DataFrames")
        
        logger.info("🔗 Next steps:")
        logger.info("   - Explore the data docs UI in your browser")
        logger.info("   - Add more sophisticated expectations")
        logger.info("   - Set up automated validation pipelines")
        logger.info("   - Integrate with your data processing workflows")
        
    else:
        logger.error("❌ Demo failed - check the logs above")