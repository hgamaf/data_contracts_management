# Data Contracts Management com OpenMetadata

Sistema para gerenciar contratos de dados usando OpenMetadata como interface principal.

## Setup Rápido

1. **Instalar OpenMetadata via Docker:**
```bash
# Baixar e rodar OpenMetadata
docker run -d -p 8585:8585 --name openmetadata openmetadata/server:latest
```

2. **Instalar dependências:**
```bash
pip install -e .
```

3. **Acessar a interface:**
- OpenMetadata UI: http://localhost:8585
- Login: admin / admin

## O que você pode fazer

✅ **Interface completa** - OpenMetadata já vem com UI pronta  
✅ **Gerenciar schemas** - Definir estruturas de dados  
✅ **Validação de qualidade** - Great Expectations integrado  
✅ **Linhagem de dados** - Rastrear origem e destino  
✅ **Governança** - Políticas e ownership  

## Próximos passos

1. Acesse http://localhost:8585
2. Crie seus datasets e schemas
3. Configure validações de qualidade
4. Use os scripts deste projeto para automação