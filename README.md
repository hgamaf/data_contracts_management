# Data Contracts Management

Sistema open source para gerenciamento de contratos de dados usando Great Expectations e integração com OpenMetadata.

## Funcionalidades

- 📋 Definição e validação de contratos de dados
- 🔍 Validação automática com Great Expectations
- 📊 Interface web para gerenciamento
- 🔗 Integração com OpenMetadata
- 📈 Dashboard de qualidade de dados
- 🚀 API REST para automação

## Instalação

```bash
# Clone o repositório
git clone <seu-repo>
cd data_contracts_management

# Instale as dependências
pip install -e .

# Para desenvolvimento
pip install -e ".[dev]"
```

## Uso Rápido

```bash
# Inicie o servidor web
uvicorn data_contracts_management.api:app --reload --port 8000

# Ou use o CLI
data-contracts --help
```

Acesse http://localhost:8000 para a interface web.

## Estrutura do Projeto

```
data_contracts_management/
├── api/              # API FastAPI
├── contracts/        # Definições de contratos
├── expectations/     # Great Expectations configs
├── web/             # Interface web
├── models/          # Modelos de dados
└── utils/           # Utilitários
```