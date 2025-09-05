# Data Contracts Management

Um sistema completo de gerenciamento de contratos de dados com validação e interface web nativa.

## 🚀 Funcionalidades

- **📋 Definição de Schemas**: Schemas de dados usando Pydantic
- **🎲 Geração de Dados**: Dados fake realistas com Faker (locale pt_BR)
- **🔍 Validação**: Great Expectations com interface web
- **🌐 Interface Web**: UI nativa do Great Expectations
- **📊 Logs Estruturados**: Logging detalhado com Loguru
- **📈 Relatórios**: Relatórios de validação em JSON

## 📦 Instalação

```bash
# Instalar dependências
uv sync

# Executar demo principal
uv run python main.py
```

## 🎯 Uso Rápido

### Executar Demo Completo
```bash
uv run python main.py
```
Este comando irá:
- Gerar dados de exemplo
- Criar expectativas de validação
- Executar validações
- Abrir a UI web automaticamente

## 📁 Estrutura do Projeto

```
data_contracts_management/
├── 📋 schema.py          # Definições de schemas (Pydantic)
├── 🚀 main.py           # Demo principal Great Expectations
├── ⚙️ config.py          # Configurações do projeto
├── 📦 pyproject.toml    # Dependências e metadados
├── 📖 README.md         # Documentação
├── 📂 data/
│   ├── 🎲 generator.py  # Gerador de dados fake (Faker)
│   └── 📋 examples.py   # Exemplos de uso
├── 📂 gx/               # Great Expectations (config + resultados)
└── 📂 logs/             # Logs e relatórios de validação
```

## 🔧 Exemplos de Uso

### Definir Schema
```python
from schema import DataSchema, SchemaField, DataType

schema = DataSchema(
    name="customer",
    fields=[
        SchemaField(
            name="customer_id",
            data_type=DataType.INTEGER,
            required=True
        ),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=True
        )
    ]
)
```

### Gerar Dados
```python
from data.generator import DataGenerator

generator = DataGenerator(locale='pt_BR')
data = generator.generate_dataset(schema, num_records=100)
```

### Validar com Great Expectations
```python
# Execute main.py para ver exemplo completo
# A UI web abrirá automaticamente
```

## 🌐 Interface Web

O Great Expectations fornece uma UI web completa com:
- 📊 Resultados de validação
- 📈 Estatísticas dos dados
- 🔍 Detalhes das expectativas
- 📋 Histórico de execuções

## 🛠️ Tecnologias

- **Pydantic**: Definição e validação de schemas
- **Faker**: Geração de dados realistas
- **Great Expectations**: Validação + UI web
- **Loguru**: Logging estruturado
- **Pandas**: Manipulação de dados

## ⚙️ Configuração

As configurações do projeto estão centralizadas no arquivo `config.py`:

```python
# Configurações principais
DEFAULT_LOCALE = "pt_BR"
DEFAULT_RECORDS = 100
LOG_LEVEL = "INFO"
DATA_DOCS_AUTO_OPEN = True
```

## 🚀 Próximos Passos

- Explorar a UI web no navegador
- Adicionar expectativas mais sofisticadas
- Configurar pipelines de validação automatizados
- Integrar com workflows de processamento de dados

## 📝 Licença

MIT License