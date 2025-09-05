"""
Gerenciador principal de contratos de dados
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import uuid

import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
import pandas as pd

from .models import DataContract, ValidationResult, FieldContract, ContractStatus


class ContractManager:
    """Gerenciador de contratos de dados com Great Expectations"""
    
    def __init__(self, contracts_dir: str = "contracts", ge_dir: str = "gx"):
        self.contracts_dir = Path(contracts_dir)
        self.ge_dir = Path(ge_dir)
        self.contracts_dir.mkdir(exist_ok=True)
        
        # Inicializar Great Expectations
        self._init_great_expectations()
        
    def _init_great_expectations(self):
        """Inicializa o contexto do Great Expectations"""
        try:
            self.context = gx.get_context(context_root_dir=str(self.ge_dir))
        except:
            # Se não existir, criar novo contexto
            self.context = gx.get_context(context_root_dir=str(self.ge_dir), mode="file")
    
    def create_contract(self, contract: DataContract) -> str:
        """Cria um novo contrato de dados"""
        if not contract.id:
            contract.id = str(uuid.uuid4())
        
        contract.created_at = datetime.now()
        contract.updated_at = datetime.now()
        
        # Salvar contrato
        contract_file = self.contracts_dir / f"{contract.id}.json"
        with open(contract_file, 'w') as f:
            json.dump(contract.dict(), f, indent=2, default=str)
        
        # Criar expectativas no Great Expectations
        self._create_expectations_suite(contract)
        
        return contract.id
    
    def get_contract(self, contract_id: str) -> Optional[DataContract]:
        """Recupera um contrato por ID"""
        contract_file = self.contracts_dir / f"{contract_id}.json"
        if not contract_file.exists():
            return None
        
        with open(contract_file, 'r') as f:
            data = json.load(f)
        
        return DataContract(**data)
    
    def list_contracts(self) -> List[DataContract]:
        """Lista todos os contratos"""
        contracts = []
        for contract_file in self.contracts_dir.glob("*.json"):
            with open(contract_file, 'r') as f:
                data = json.load(f)
            contracts.append(DataContract(**data))
        
        return sorted(contracts, key=lambda x: x.created_at or datetime.min, reverse=True)
    
    def update_contract(self, contract_id: str, contract: DataContract) -> bool:
        """Atualiza um contrato existente"""
        if not self.get_contract(contract_id):
            return False
        
        contract.id = contract_id
        contract.updated_at = datetime.now()
        
        contract_file = self.contracts_dir / f"{contract_id}.json"
        with open(contract_file, 'w') as f:
            json.dump(contract.dict(), f, indent=2, default=str)
        
        # Atualizar expectativas
        self._create_expectations_suite(contract)
        
        return True
    
    def delete_contract(self, contract_id: str) -> bool:
        """Remove um contrato"""
        contract_file = self.contracts_dir / f"{contract_id}.json"
        if not contract_file.exists():
            return False
        
        contract_file.unlink()
        
        # Remover suite de expectativas
        try:
            self.context.delete_expectation_suite(f"contract_{contract_id}")
        except:
            pass
        
        return True    
  
  def _create_expectations_suite(self, contract: DataContract):
        """Cria suite de expectativas no Great Expectations"""
        suite_name = f"contract_{contract.id}"
        
        try:
            # Criar ou atualizar suite
            suite = self.context.add_expectation_suite(
                expectation_suite_name=suite_name
            )
        except:
            # Suite já existe, recuperar
            suite = self.context.get_expectation_suite(suite_name)
            suite.expectations = []  # Limpar expectativas existentes
        
        # Adicionar expectativas baseadas nos campos do contrato
        for field in contract.fields:
            # Expectativa de existência da coluna
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": field.name}
                )
            )
            
            # Expectativa de tipo de dados
            if field.data_type.value in ["integer", "float"]:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_of_type",
                        kwargs={
                            "column": field.name,
                            "type_": "int64" if field.data_type.value == "integer" else "float64"
                        }
                    )
                )
            
            # Expectativa de nulidade
            if not field.nullable:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_not_be_null",
                        kwargs={"column": field.name}
                    )
                )
            
            # Adicionar constraints específicos
            for constraint_name, constraint_value in field.constraints.items():
                if constraint_name == "min_value" and field.data_type.value in ["integer", "float"]:
                    suite.add_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_between",
                            kwargs={
                                "column": field.name,
                                "min_value": constraint_value
                            }
                        )
                    )
                elif constraint_name == "max_value" and field.data_type.value in ["integer", "float"]:
                    suite.add_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_between",
                            kwargs={
                                "column": field.name,
                                "max_value": constraint_value
                            }
                        )
                    )
                elif constraint_name == "allowed_values":
                    suite.add_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_in_set",
                            kwargs={
                                "column": field.name,
                                "value_set": constraint_value
                            }
                        )
                    )
        
        # Salvar suite
        self.context.save_expectation_suite(suite)
    
    def validate_data(self, contract_id: str, data_path: str) -> ValidationResult:
        """Valida dados contra um contrato"""
        contract = self.get_contract(contract_id)
        if not contract:
            return ValidationResult(
                contract_id=contract_id,
                success=False,
                timestamp=datetime.now(),
                details={},
                errors=["Contrato não encontrado"]
            )
        
        try:
            # Carregar dados
            if data_path.endswith('.csv'):
                df = pd.read_csv(data_path)
            elif data_path.endswith('.json'):
                df = pd.read_json(data_path)
            else:
                return ValidationResult(
                    contract_id=contract_id,
                    success=False,
                    timestamp=datetime.now(),
                    details={},
                    errors=["Formato de arquivo não suportado"]
                )
            
            # Criar datasource temporário
            datasource_name = f"temp_datasource_{contract_id}"
            suite_name = f"contract_{contract_id}"
            
            # Executar validação
            validator = self.context.get_validator(
                batch_request=self.context.sources.pandas_default.read_dataframe(df),
                expectation_suite_name=suite_name
            )
            
            results = validator.validate()
            
            # Processar resultados
            errors = []
            warnings = []
            
            for result in results.results:
                if not result.success:
                    errors.append(f"Falha na expectativa: {result.expectation_config.expectation_type}")
            
            return ValidationResult(
                contract_id=contract_id,
                success=results.success,
                timestamp=datetime.now(),
                details=results.to_json_dict(),
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            return ValidationResult(
                contract_id=contract_id,
                success=False,
                timestamp=datetime.now(),
                details={},
                errors=[f"Erro na validação: {str(e)}"]
            )