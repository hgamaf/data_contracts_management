"""
Integração com OpenMetadata
"""
import json
import requests
from typing import Optional, Dict, Any
import logging

from .models import DataContract, OpenMetadataConfig

logger = logging.getLogger(__name__)


class OpenMetadataIntegration:
    """Integração com OpenMetadata para sincronização de contratos"""
    
    def __init__(self, config_file: str = "openmetadata_config.json"):
        self.config = self._load_config(config_file)
        self.session = requests.Session()
        
        if self.config.auth_token:
            self.session.headers.update({
                "Authorization": f"Bearer {self.config.auth_token}"
            })
    
    def _load_config(self, config_file: str) -> OpenMetadataConfig:
        """Carrega configuração do OpenMetadata"""
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            return OpenMetadataConfig(**config_data)
        except FileNotFoundError:
            # Criar configuração padrão
            default_config = OpenMetadataConfig()
            with open(config_file, 'w') as f:
                json.dump(default_config.dict(), f, indent=2)
            return default_config
    
    def is_enabled(self) -> bool:
        """Verifica se a integração está habilitada"""
        return self.config.enabled
    
    async def sync_contract(self, contract: DataContract) -> bool:
        """Sincroniza contrato com OpenMetadata"""
        if not self.is_enabled():
            return False
        
        try:
            # Converter contrato para formato OpenMetadata
            om_table = self._contract_to_openmetadata(contract)
            
            # Enviar para OpenMetadata
            response = self.session.post(
                f"{self.config.server_url}/api/v1/tables",
                json=om_table,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Contrato {contract.id} sincronizado com OpenMetadata")
                return True
            else:
                logger.error(f"Erro ao sincronizar contrato: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Erro na integração com OpenMetadata: {str(e)}")
            return False
    
    def _contract_to_openmetadata(self, contract: DataContract) -> Dict[str, Any]:
        """Converte contrato para formato OpenMetadata"""
        columns = []
        
        for field in contract.fields:
            column = {
                "name": field.name,
                "dataType": self._map_data_type(field.data_type.value),
                "description": field.description or "",
                "constraint": "NOT_NULL" if not field.nullable else "NULL"
            }
            columns.append(column)
        
        return {
            "name": contract.dataset_name,
            "displayName": contract.name,
            "description": contract.description or "",
            "columns": columns,
            "tableType": "Regular",
            "service": {
                "id": self.config.service_name,
                "type": "databaseService"
            },
            "database": {
                "name": "data_contracts",
                "service": {
                    "id": self.config.service_name,
                    "type": "databaseService"
                }
            },
            "databaseSchema": {
                "name": "contracts",
                "database": {
                    "name": "data_contracts",
                    "service": {
                        "id": self.config.service_name,
                        "type": "databaseService"
                    }
                }
            },
            "tags": [{"tagFQN": tag} for tag in contract.tags],
            "owner": {
                "name": contract.owner or "data-team",
                "type": "team"
            }
        }
    
    def _map_data_type(self, data_type: str) -> str:
        """Mapeia tipos de dados para OpenMetadata"""
        mapping = {
            "string": "VARCHAR",
            "integer": "INT",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "datetime": "TIMESTAMP"
        }
        return mapping.get(data_type, "VARCHAR")
    
    async def get_lineage(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Recupera linhagem de dados do OpenMetadata"""
        if not self.is_enabled():
            return None
        
        try:
            response = self.session.get(
                f"{self.config.server_url}/api/v1/lineage/table/name/{dataset_name}"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Linhagem não encontrada para {dataset_name}")
                return None
                
        except Exception as e:
            logger.error(f"Erro ao recuperar linhagem: {str(e)}")
            return None
    
    async def test_connection(self) -> bool:
        """Testa conexão com OpenMetadata"""
        try:
            response = self.session.get(f"{self.config.server_url}/api/v1/system/version")
            return response.status_code == 200
        except:
            return False