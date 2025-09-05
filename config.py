"""
Configurações do projeto Data Contracts Management
"""
from pathlib import Path

# Diretórios do projeto
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
GX_DIR = PROJECT_ROOT / "gx"

# Configurações de geração de dados
DEFAULT_LOCALE = "pt_BR"
DEFAULT_RECORDS = 100

# Configurações de logging
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"
LOG_LEVEL = "INFO"

# Configurações Great Expectations
GX_CONTEXT_ROOT_DIR = str(GX_DIR)
DATA_DOCS_AUTO_OPEN = True

# Criar diretórios se não existirem
LOGS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)