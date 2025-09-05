#!/usr/bin/env python3
"""
Script para configurar dados de exemplo no OpenMetadata
"""
import json
import requests
from pathlib import Path


class OpenMetadataSetup:
    def __init__(self, base_url="http://localhost:8585"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def create_sample_database(self):
        """Cria um database de exemplo"""
        database_data = {
            "name": "data_contracts_db",
            "displayName": "Data Contracts Database",
            "description": "Database para contratos de dados",
            "service": {
                "id": "sample-db-service",
                "type": "databaseService"
            }
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/databases",
            json=database_data
        )
        
        if response.status_code in [200, 201]:
            print("✅ Database criado com sucesso")
            return response.json()
        else:
            print(f"❌ Erro ao criar database: {response.text}")
            return None
    
    def create_sample_table(self):
        """Cria uma tabela de exemplo com contrato de dados"""
        table_data = {
            "name": "user_events",
            "displayName": "User Events",
            "description": "Eventos de usuário com contrato de dados definido",
            "columns": [
                {
                    "name": "user_id",
                    "dataType": "INT",
                    "description": "ID único do usuário",
                    "constraint": "NOT_NULL"
                },
                {
                    "name": "event_type",
                    "dataType": "VARCHAR",
                    "description": "Tipo do evento (login, logout, click, etc)",
                    "constraint": "NOT_NULL"
                },
                {
                    "name": "timestamp",
                    "dataType": "TIMESTAMP",
                    "description": "Timestamp do evento",
                    "constraint": "NOT_NULL"
                },
                {
                    "name": "properties",
                    "dataType": "JSON",
                    "description": "Propriedades adicionais do evento",
                    "constraint": "NULL"
                }
            ],
            "tableType": "Regular",
            "service": {
                "id": "sample-db-service",
                "type": "databaseService"
            },
            "database": {
                "name": "data_contracts_db"
            },
            "databaseSchema": {
                "name": "events"
            }
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/tables",
            json=table_data
        )
        
        if response.status_code in [200, 201]:
            print("✅ Tabela criada com sucesso")
            return response.json()
        else:
            print(f"❌ Erro ao criar tabela: {response.text}")
            return None


def main():
    """Configura dados de exemplo"""
    print("🔧 Configurando dados de exemplo no OpenMetadata...")
    
    setup = OpenMetadataSetup()
    
    # Verificar se OpenMetadata está rodando
    try:
        response = requests.get("http://localhost:8585/api/v1/system/version")
        if response.status_code != 200:
            print("❌ OpenMetadata não está rodando. Execute 'python main.py' primeiro.")
            return
    except:
        print("❌ Não foi possível conectar ao OpenMetadata")
        return
    
    # Criar dados de exemplo
    setup.create_sample_database()
    setup.create_sample_table()
    
    print("\n🎉 Dados de exemplo configurados!")
    print("📊 Acesse http://localhost:8585 para ver")


if __name__ == "__main__":
    main()