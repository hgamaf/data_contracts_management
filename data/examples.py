"""
Example usage of the data generator.
"""
from generator import DataGenerator, create_customer_schema, create_order_schema
from schema import DataContract
import json


def generate_sample_data():
    """Generate sample data for demonstration."""
    generator = DataGenerator(locale='pt_BR')
    
    # Generate customer data
    customer_schema = create_customer_schema()
    print(f"Generating customer data using schema: {customer_schema.name}")
    
    # Generate single record
    customer_record = generator.generate_record(customer_schema)
    print("Sample customer record:")
    print(json.dumps(customer_record, indent=2, default=str, ensure_ascii=False))
    
    # Generate dataset
    customer_file = generator.generate_json_file(customer_schema, num_records=50)
    print(f"Generated customer dataset: {customer_file}")
    
    # Generate order data
    order_schema = create_order_schema()
    print(f"\nGenerating order data using schema: {order_schema.name}")
    
    order_record = generator.generate_record(order_schema)
    print("Sample order record:")
    print(json.dumps(order_record, indent=2, default=str, ensure_ascii=False))
    
    order_file = generator.generate_json_file(order_schema, num_records=100)
    print(f"Generated order dataset: {order_file}")
    
    # Create data contracts
    customer_contract = DataContract(
        id="contract_customer_001",
        name="Customer Data Contract",
        schema=customer_schema,
        owner="data_team@company.com",
        status="active",
        metadata={
            "source_system": "CRM",
            "update_frequency": "daily",
            "data_classification": "PII"
        }
    )
    
    order_contract = DataContract(
        id="contract_order_001",
        name="Order Data Contract",
        schema=order_schema,
        owner="sales_team@company.com",
        status="active",
        metadata={
            "source_system": "E-commerce",
            "update_frequency": "real-time",
            "data_classification": "business"
        }
    )
    
    print(f"\nCreated contracts:")
    print(f"- {customer_contract.name} (ID: {customer_contract.id})")
    print(f"- {order_contract.name} (ID: {order_contract.id})")


if __name__ == "__main__":
    generate_sample_data()