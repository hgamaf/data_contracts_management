# Data Contracts Management

Sistema simples para gerenciar contratos de dados usando Great Expectations com UI nativa.

## 🚀 Setup Super Simples

```bash
# 1. Instalar dependências
uv sync

# 2. Inicializar Great Expectations
uv run great_expectations init

# 3. Criar dados de exemplo
uv run python main.py

# 4. Ver UI no navegador
uv run great_expectations docs build --site-name local
```

## ✅ O que você ganha

- **UI Web Nativa** - Interface automática do Great Expectations
- **Validação de Dados** - Schemas e regras de qualidade
- **Relatórios Visuais** - Dashboards automáticos
- **Documentação** - Docs geradas automaticamente

## 📊 Acesso

Após executar os comandos acima:
- **UI Principal**: http://localhost:8080
- **Documentação**: Gerada automaticamente

## 💡 Uso

1. Coloque seus arquivos CSV/JSON na pasta `data/`
2. Execute `uv run python main.py` para validar
3. Veja os resultados na UI web