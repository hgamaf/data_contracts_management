"""
Data generator using Faker for creating sample data based on schemas.
"""
from faker import Faker
from typing import List, Dict, Any, Optional
import random
from datetime import datetime, timedelta
import json

from schema import DataSchema, SchemaField, DataType, DataContract


class DataGenerator:
    """Generate fake data based on schema definitions."""
    
    def __init__(self, locale: str = 'pt_BR'):
        """Initialize the data generator with specified locale."""
        self.fake = Faker(locale)
        
    def generate_field_value(self, field: SchemaField) -> Any:
        """Generate a single field value based on field definition."""
        if not field.required and random.random() < 0.1:  # 10% chance of None for optional fields
            return None
            
        if field.data_type == DataType.STRING:
            return self._generate_string_value(field)
        elif field.data_type == DataType.INTEGER:
            return self._generate_integer_value(field)
        elif field.data_type == DataType.FLOAT:
            return self._generate_float_value(field)
        elif field.data_type == DataType.BOOLEAN:
            return self.fake.boolean()
        elif field.data_type == DataType.DATE:
            return self.fake.date_between(start_date='-2y', end_date='today').isoformat()
        elif field.data_type == DataType.DATETIME:
            return self.fake.date_time_between(start_date='-2y', end_date='now').isoformat()
        elif field.data_type == DataType.ARRAY:
            return self._generate_array_value(field)
        elif field.data_type == DataType.OBJECT:
            return self._generate_object_value(field)
        else:
            return field.default_value
    
    def _generate_string_value(self, field: SchemaField) -> str:
        """Generate string value based on field name and validation rules."""
        field_name = field.name.lower()
        
        # Smart generation based on field name
        if 'email' in field_name:
            return self.fake.email()
        elif 'name' in field_name and 'first' in field_name:
            return self.fake.first_name()
        elif 'name' in field_name and 'last' in field_name:
            return self.fake.last_name()
        elif 'name' in field_name:
            return self.fake.name()
        elif 'phone' in field_name or 'telefone' in field_name:
            return self.fake.phone_number()
        elif 'address' in field_name or 'endereco' in field_name:
            return self.fake.address()
        elif 'city' in field_name or 'cidade' in field_name:
            return self.fake.city()
        elif 'company' in field_name or 'empresa' in field_name:
            return self.fake.company()
        elif 'description' in field_name or 'descricao' in field_name:
            return self.fake.text(max_nb_chars=200)
        elif 'url' in field_name or 'website' in field_name:
            return self.fake.url()
        elif 'cpf' in field_name:
            return self.fake.cpf()
        elif 'cnpj' in field_name:
            return self.fake.cnpj()
        else:
            # Apply validation rules if any
            min_length = 5
            max_length = 50
            
            for rule in field.validation_rules:
                if rule.type == "min_length":
                    min_length = max(min_length, rule.value)
                elif rule.type == "max_length":
                    max_length = min(max_length, rule.value)
            
            return self.fake.text(max_nb_chars=random.randint(min_length, max_length))
    
    def _generate_integer_value(self, field: SchemaField) -> int:
        """Generate integer value based on validation rules."""
        min_val = 0
        max_val = 1000000
        
        for rule in field.validation_rules:
            if rule.type == "min_value":
                min_val = rule.value
            elif rule.type == "max_value":
                max_val = rule.value
        
        return random.randint(min_val, max_val)
    
    def _generate_float_value(self, field: SchemaField) -> float:
        """Generate float value based on validation rules."""
        min_val = 0.0
        max_val = 1000000.0
        
        for rule in field.validation_rules:
            if rule.type == "min_value":
                min_val = rule.value
            elif rule.type == "max_value":
                max_val = rule.value
        
        return round(random.uniform(min_val, max_val), 2)
    
    def _generate_array_value(self, field: SchemaField) -> List[Any]:
        """Generate array value."""
        size = random.randint(1, 5)
        return [self.fake.word() for _ in range(size)]
    
    def _generate_object_value(self, field: SchemaField) -> Dict[str, Any]:
        """Generate object value."""
        return {
            "id": self.fake.uuid4(),
            "name": self.fake.word(),
            "value": self.fake.random_number()
        }
    
    def generate_record(self, schema: DataSchema) -> Dict[str, Any]:
        """Generate a single record based on schema."""
        record = {}
        
        for field in schema.fields:
            record[field.name] = self.generate_field_value(field)
        
        return record
    
    def generate_dataset(self, schema: DataSchema, num_records: int = 100) -> List[Dict[str, Any]]:
        """Generate multiple records based on schema."""
        return [self.generate_record(schema) for _ in range(num_records)]
    
    def generate_json_file(self, schema: DataSchema, num_records: int = 100, filename: Optional[str] = None) -> str:
        """Generate dataset and save to JSON file."""
        dataset = self.generate_dataset(schema, num_records)
        
        if filename is None:
            filename = f"data_contracts_management/data/generated_{schema.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, indent=2, ensure_ascii=False, default=str)
        
        return filename
    
    def generate_csv_file(self, schema: DataSchema, num_records: int = 100, filename: Optional[str] = None) -> str:
        """Generate dataset and save to CSV file."""
        import pandas as pd
        
        dataset = self.generate_dataset(schema, num_records)
        df = pd.DataFrame(dataset)
        
        if filename is None:
            filename = f"data_contracts_management/data/generated_{schema.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        df.to_csv(filename, index=False, encoding='utf-8')
        
        return filename


# Predefined schema examples
def create_customer_schema() -> DataSchema:
    """Create a sample customer schema."""
    from schema import SchemaField, DataType, COMMON_VALIDATION_RULES
    
    fields = [
        SchemaField(
            name="customer_id",
            data_type=DataType.INTEGER,
            required=True,
            description="Unique customer identifier",
            validation_rules=[COMMON_VALIDATION_RULES["not_null"], COMMON_VALIDATION_RULES["unique"]]
        ),
        SchemaField(
            name="first_name",
            data_type=DataType.STRING,
            required=True,
            description="Customer first name",
            validation_rules=[COMMON_VALIDATION_RULES["min_length"](2), COMMON_VALIDATION_RULES["max_length"](50)]
        ),
        SchemaField(
            name="last_name",
            data_type=DataType.STRING,
            required=True,
            description="Customer last name",
            validation_rules=[COMMON_VALIDATION_RULES["min_length"](2), COMMON_VALIDATION_RULES["max_length"](50)]
        ),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=True,
            description="Customer email address",
            validation_rules=[COMMON_VALIDATION_RULES["unique"]]
        ),
        SchemaField(
            name="phone",
            data_type=DataType.STRING,
            required=False,
            description="Customer phone number"
        ),
        SchemaField(
            name="birth_date",
            data_type=DataType.DATE,
            required=False,
            description="Customer birth date"
        ),
        SchemaField(
            name="created_at",
            data_type=DataType.DATETIME,
            required=True,
            description="Record creation timestamp"
        ),
        SchemaField(
            name="is_active",
            data_type=DataType.BOOLEAN,
            required=True,
            description="Whether customer is active",
            default_value=True
        )
    ]
    
    return DataSchema(
        name="customer",
        version="1.0.0",
        description="Customer data schema",
        fields=fields,
        tags=["customer", "crm", "personal_data"]
    )


def create_order_schema() -> DataSchema:
    """Create a sample order schema."""
    from schema import SchemaField, DataType, COMMON_VALIDATION_RULES
    
    fields = [
        SchemaField(
            name="order_id",
            data_type=DataType.STRING,
            required=True,
            description="Unique order identifier",
            validation_rules=[COMMON_VALIDATION_RULES["not_null"], COMMON_VALIDATION_RULES["unique"]]
        ),
        SchemaField(
            name="customer_id",
            data_type=DataType.INTEGER,
            required=True,
            description="Customer identifier"
        ),
        SchemaField(
            name="order_date",
            data_type=DataType.DATETIME,
            required=True,
            description="Order creation date"
        ),
        SchemaField(
            name="total_amount",
            data_type=DataType.FLOAT,
            required=True,
            description="Total order amount",
            validation_rules=[COMMON_VALIDATION_RULES["min_value"](0)]
        ),
        SchemaField(
            name="status",
            data_type=DataType.STRING,
            required=True,
            description="Order status",
            default_value="pending"
        ),
        SchemaField(
            name="items",
            data_type=DataType.ARRAY,
            required=True,
            description="Order items"
        )
    ]
    
    return DataSchema(
        name="order",
        version="1.0.0",
        description="Order data schema",
        fields=fields,
        tags=["order", "sales", "transaction"]
    )