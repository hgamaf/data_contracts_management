"""
Example of data validation using Pandera and Loguru.
"""
import pandas as pd
from validator import DataValidator
from data.generator import DataGenerator, create_customer_schema, create_order_schema
from schema import DataContract
from loguru import logger


def run_validation_example():
    """Run complete validation example."""
    logger.info("🚀 Starting data validation example")
    
    # Initialize components
    generator = DataGenerator(locale='pt_BR')
    validator = DataValidator(log_level="INFO")
    
    # Create schemas and contracts
    customer_schema = create_customer_schema()
    order_schema = create_order_schema()
    
    customer_contract = DataContract(
        id="contract_customer_001",
        name="Customer Data Contract",
        schema=customer_schema,
        owner="data_team@company.com",
        status="active"
    )
    
    order_contract = DataContract(
        id="contract_order_001", 
        name="Order Data Contract",
        schema=order_schema,
        owner="sales_team@company.com",
        status="active"
    )
    
    logger.info("📋 Created data contracts")
    
    # Generate valid data
    logger.info("🎲 Generating valid customer data...")
    customer_data = generator.generate_dataset(customer_schema, num_records=100)
    customer_df = pd.DataFrame(customer_data)
    
    logger.info("🎲 Generating valid order data...")
    order_data = generator.generate_dataset(order_schema, num_records=150)
    order_df = pd.DataFrame(order_data)
    
    # Validate customer data
    logger.info("🔍 Validating customer data...")
    is_valid, customer_result = validator.validate_contract_data(customer_df, customer_contract)
    
    if is_valid:
        logger.success("✅ Customer data validation passed!")
    else:
        logger.error("❌ Customer data validation failed!")
    
    # Save customer validation report
    customer_report_file = validator.save_validation_report(customer_result)
    
    # Validate order data
    logger.info("🔍 Validating order data...")
    is_valid, order_result = validator.validate_contract_data(order_df, order_contract)
    
    if is_valid:
        logger.success("✅ Order data validation passed!")
    else:
        logger.error("❌ Order data validation failed!")
    
    # Save order validation report
    order_report_file = validator.save_validation_report(order_result)
    
    # Create intentionally invalid data for testing
    logger.info("🧪 Testing with invalid data...")
    
    # Create invalid customer data
    invalid_customer_data = customer_data.copy()[:10]
    
    # Introduce errors
    invalid_customer_data[0]['email'] = "invalid-email"  # Invalid email format
    invalid_customer_data[1]['first_name'] = ""  # Empty required field
    invalid_customer_data[2]['customer_id'] = -1  # Negative ID
    
    invalid_customer_df = pd.DataFrame(invalid_customer_data)
    
    logger.info("🔍 Validating invalid customer data...")
    is_valid, invalid_result = validator.validate_contract_data(invalid_customer_df, customer_contract)
    
    if not is_valid:
        logger.info("✅ Invalid data correctly detected!")
        logger.info(f"Found {len(invalid_result['errors'])} validation errors")
    
    # Save invalid data report
    invalid_report_file = validator.save_validation_report(invalid_result)
    
    # Summary
    logger.info("📊 Validation Summary:")
    logger.info(f"   Customer validation: {'✅ PASSED' if customer_result['is_valid'] else '❌ FAILED'}")
    logger.info(f"   Order validation: {'✅ PASSED' if order_result['is_valid'] else '❌ FAILED'}")
    logger.info(f"   Invalid data test: {'✅ DETECTED' if not invalid_result['is_valid'] else '❌ NOT DETECTED'}")
    
    logger.info("📁 Generated files:")
    logger.info(f"   Customer report: {customer_report_file}")
    logger.info(f"   Order report: {order_report_file}")
    logger.info(f"   Invalid data report: {invalid_report_file}")
    
    logger.success("🎉 Validation example completed!")


def test_schema_conversion():
    """Test schema conversion to Pandera."""
    logger.info("🔧 Testing schema conversion...")
    
    validator = DataValidator()
    customer_schema = create_customer_schema()
    
    # Convert to Pandera
    pandera_schema = validator.convert_schema_to_pandera(customer_schema)
    
    logger.info(f"✅ Successfully converted schema '{customer_schema.name}'")
    logger.info(f"   Original fields: {len(customer_schema.fields)}")
    logger.info(f"   Pandera columns: {len(pandera_schema.columns)}")
    
    # Print schema details
    for field_name, column in pandera_schema.columns.items():
        logger.debug(f"   {field_name}: {column.dtype}, nullable={column.nullable}")


if __name__ == "__main__":
    # Run tests
    test_schema_conversion()
    print("\n" + "="*50 + "\n")
    run_validation_example()