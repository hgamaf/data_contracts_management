"""
API FastAPI para gerenciamento de contratos de dados
"""
from typing import List, Optional
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import tempfile
import os

from .models import DataContract, ValidationResult, ContractStatus
from .contract_manager import ContractManager
from .openmetadata_integration import OpenMetadataIntegration

app = FastAPI(
    title="Data Contracts Management",
    description="Sistema de gerenciamento de contratos de dados",
    version="0.1.0"
)

# Inicializar gerenciador
contract_manager = ContractManager()
openmetadata = OpenMetadataIntegration()

# Servir arquivos estáticos
app.mount("/static", StaticFiles(directory="data_contracts_management/web/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Página principal"""
    return FileResponse("data_contracts_management/web/index.html")


@app.get("/contracts", response_model=List[DataContract])
async def list_contracts():
    """Lista todos os contratos"""
    return contract_manager.list_contracts()


@app.post("/contracts", response_model=dict)
async def create_contract(contract: DataContract):
    """Cria um novo contrato"""
    try:
        contract_id = contract_manager.create_contract(contract)
        
        # Sincronizar com OpenMetadata se habilitado
        if openmetadata.is_enabled():
            await openmetadata.sync_contract(contract)
        
        return {"id": contract_id, "message": "Contrato criado com sucesso"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/contracts/{contract_id}", response_model=DataContract)
async def get_contract(contract_id: str):
    """Recupera um contrato específico"""
    contract = contract_manager.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")
    return contract


@app.put("/contracts/{contract_id}", response_model=dict)
async def update_contract(contract_id: str, contract: DataContract):
    """Atualiza um contrato existente"""
    if not contract_manager.update_contract(contract_id, contract):
        raise HTTPException(status_code=404, detail="Contrato não encontrado")
    
    # Sincronizar com OpenMetadata se habilitado
    if openmetadata.is_enabled():
        await openmetadata.sync_contract(contract)
    
    return {"message": "Contrato atualizado com sucesso"}


@app.delete("/contracts/{contract_id}", response_model=dict)
async def delete_contract(contract_id: str):
    """Remove um contrato"""
    if not contract_manager.delete_contract(contract_id):
        raise HTTPException(status_code=404, detail="Contrato não encontrado")
    return {"message": "Contrato removido com sucesso"}


@app.post("/contracts/{contract_id}/validate", response_model=ValidationResult)
async def validate_contract(contract_id: str, file: UploadFile = File(...)):
    """Valida dados contra um contrato"""
    # Salvar arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file.filename.split('.')[-1]}") as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name
    
    try:
        result = contract_manager.validate_data(contract_id, tmp_path)
        return result
    finally:
        # Limpar arquivo temporário
        os.unlink(tmp_path)


@app.get("/contracts/{contract_id}/status", response_model=dict)
async def get_contract_status(contract_id: str):
    """Recupera o status de um contrato"""
    contract = contract_manager.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")
    
    return {
        "contract_id": contract_id,
        "status": contract.status,
        "last_updated": contract.updated_at
    }


@app.post("/contracts/{contract_id}/activate", response_model=dict)
async def activate_contract(contract_id: str):
    """Ativa um contrato"""
    contract = contract_manager.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contrato não encontrado")
    
    contract.status = ContractStatus.ACTIVE
    contract_manager.update_contract(contract_id, contract)
    
    return {"message": "Contrato ativado com sucesso"}


@app.get("/health")
async def health_check():
    """Health check da API"""
    return {
        "status": "healthy",
        "contracts_count": len(contract_manager.list_contracts()),
        "openmetadata_enabled": openmetadata.is_enabled()
    }