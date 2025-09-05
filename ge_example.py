"""
Example using Great Expectations with UI for data validation.
"""
import pandas as pd
from loguru import logger
from ge_integration import GreatExpectationsValidator, setup_great_expectations_project
from data.generator import DataGenerator, create_customer_schema, create_order_schema
from schema import DataContract
import time


def run_great_expectations_example():
    """Run complete Great Expectations validation example with UI."""
    logger.info("🚀 Starting Great Expectations validation example")
    
    # Setup Great Expectations
    ge_validator = setup_great_expectations_project()
    
    # Initialize data generator
    generator = DataGenerator(locale='pt_BR')
    
    # Create schemas and contracts
    customer_schema = create_customer_schema()
    order_schema = create_order_schema()
    
    customer_contract = DataContract(
        id="contract_customer_001",
        name="Customer Data Contract",
        data_schema=customer_schema,
        owner="data_team@company.com",
        status="active"
    )
    
    order_contract = DataContract(
        id="contract_order_001",
        name="Order Data Contract", 
        data_schema=order_schema,
        owner="sales_team@company.com",
        status="active"
    )
    
    logger.info("📋 Created data contracts")
    
    # Generate test data
    logger.info("🎲 Generating customer data...")
    customer_data = generator.generate_dataset(customer_schema, num_records=100)
    customer_df = pd.DataFrame(customer_data)
    
    logger.info("🎲 Generating order data...")
    order_data = generator.generate_dataset(order_schema, num_records=150)
    order_df = pd.DataFrame(order_data)
    
    # Create expectation suites
    logger.info("📝 Creating expectation suites...")
    customer_suite = ge_validator.create_expectation_suite(customer_schema, "customer_expectations")
    order_suite = ge_validator.create_expectation_suite(order_schema, "order_expectations")
    
    # Validate customer data
    logger.info("🔍 Validating customer data with Great Expectations...")
    customer_result = ge_validator.validate_contract_data(customer_df, customer_contract)
    
    if customer_result["success"]:
        logger.success("✅ Customer data validation passed!")
    else:
        logger.error(f"❌ Customer data validation failed! {customer_result['failed_expectations']} expectations failed")
    
    # Validate order data
    logger.info("🔍 Validating order data with Great Expectations...")
    order_result = ge_validator.validate_contract_data(order_df, order_contract)
    
    if order_result["success"]:
        logger.success("✅ Order data validation passed!")
    else:
        logger.error(f"❌ Order data validation failed! {order_result['failed_expectations']} expectations failed")
    
    # Create checkpoints for automated validation
    logger.info("⚙️ Creating checkpoints...")
    customer_checkpoint = ge_validator.create_checkpoint(customer_suite)
    order_checkpoint = ge_validator.create_checkpoint(order_suite)
    
    # Test with invalid data
    logger.info("🧪 Testing with invalid data...")
    
    # Create invalid customer data
    invalid_customer_data = customer_data.copy()[:20]
    
    # Introduce various errors
    for i in range(5):
        invalid_customer_data[i]['email'] = f"invalid-email-{i}"  # Invalid email
        invalid_customer_data[i]['first_name'] = ""  # Empty required field
        invalid_customer_data[i]['customer_id'] = None  # Null required field
    
    invalid_customer_df = pd.DataFrame(invalid_customer_data)
    
    logger.info("🔍 Validating invalid customer data...")
    invalid_result = ge_validator.validate_contract_data(invalid_customer_df, customer_contract)
    
    if not invalid_result["success"]:
        logger.success(f"✅ Invalid data correctly detected! {invalid_result['failed_expectations']} expectations failed as expected")
    else:
        logger.warning("⚠️ Invalid data was not detected - this might indicate an issue with expectations")
    
    # Build and show data docs
    logger.info("📊 Building Great Expectations data docs...")
    ge_validator.build_data_docs()
    
    # Get validation results summary
    logger.info("📈 Retrieving validation results...")
    all_results = ge_validator.get_validation_results()
    
    logger.info("📊 Validation Summary:")
    logger.info(f"   Total validation runs: {len(all_results)}")
    
    successful_runs = sum(1 for r in all_results if r['success'])
    failed_runs = len(all_results) - successful_runs
    
    logger.info(f"   Successful runs: {successful_runs}")
    logger.info(f"   Failed runs: {failed_runs}")
    
    # Show recent results
    if all_results:
        logger.info("📋 Recent validation results:")
        for result in all_results[-5:]:  # Show last 5 results
            status = "✅ PASSED" if result['success'] else "❌ FAILED"
            logger.info(f"   {result['suite_name']}: {status} ({result['timestamp']})")
    
    # Start UI server
    logger.info("🌐 Starting Great Expectations UI...")
    ge_validator.start_ui_server()
    
    logger.success("🎉 Great Expectations validation example completed!")
    logger.info("💡 Tips:")
    logger.info("   - Open the data docs URL in your browser to explore validation results")
    logger.info("   - The UI shows detailed expectation results, data profiling, and validation history")
    logger.info("   - You can create custom expectations and run validations through the UI")
    
    return ge_validator


def demonstrate_continuous_validation():
    """Demonstrate continuous validation with multiple data batches."""
    logger.info("🔄 Demonstrating continuous validation...")
    
    ge_validator = GreatExpectationsValidator()
    generator = DataGenerator(locale='pt_BR')
    customer_schema = create_customer_schema()
    
    customer_contract = DataContract(
        id="contract_customer_continuous",
        name="Customer Continuous Validation",
        data_schema=customer_schema,
        owner="data_team@company.com",
        status="active"
    )
    
    # Create expectation suite
    suite_name = ge_validator.create_expectation_suite(customer_schema, "customer_continuous_suite")
    
    # Simulate multiple data batches
    for batch_num in range(1, 4):
        logger.info(f"📦 Processing batch {batch_num}...")
        
        # Generate data with varying quality
        num_records = 50
        data = generator.generate_dataset(customer_schema, num_records)
        
        # Introduce some issues in later batches
        if batch_num > 1:
            for i in range(batch_num * 2):  # More issues in later batches
                if i < len(data):
                    data[i]['email'] = f"bad-email-{i}"
        
        df = pd.DataFrame(data)
        
        # Validate batch
        result = ge_validator.validate_dataframe(df, suite_name, f"batch_{batch_num}")
        
        status = "✅ PASSED" if result["success"] else "❌ FAILED"
        logger.info(f"   Batch {batch_num}: {status} ({result['successful_expectations']}/{result['total_expectations']} expectations)")
        
        # Small delay to simulate real-world timing
        time.sleep(1)
    
    # Update data docs with all results
    ge_validator.build_data_docs()
    
    logger.success("🔄 Continuous validation demonstration completed!")


if __name__ == "__main__":
    # Run main example
    ge_validator = run_great_expectations_example()
    
    print("\n" + "="*60 + "\n")
    
    # Run continuous validation demo
    demonstrate_continuous_validation()