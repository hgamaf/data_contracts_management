#!/usr/bin/env python3
"""
Script principal para iniciar o OpenMetadata e configurar o ambiente
"""
import subprocess
import sys
import time
import requests


def check_docker():
    """Verifica se Docker está instalado e rodando"""
    try:
        subprocess.run(["docker", "--version"], check=True, capture_output=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def start_openmetadata():
    """Inicia o OpenMetadata via Docker"""
    print("🚀 Iniciando OpenMetadata...")
    
    # Verificar se já está rodando
    try:
        response = requests.get("http://localhost:8585/api/v1/system/version", timeout=5)
        if response.status_code == 200:
            print("✅ OpenMetadata já está rodando!")
            return True
    except:
        pass
    
    # Iniciar container
    cmd = [
        "docker", "run", "-d",
        "-p", "8585:8585",
        "--name", "openmetadata",
        "openmetadata/server:latest"
    ]
    
    try:
        subprocess.run(cmd, check=True)
        print("⏳ Aguardando OpenMetadata inicializar...")
        
        # Aguardar até estar pronto
        for i in range(60):  # 60 tentativas = 2 minutos
            try:
                response = requests.get("http://localhost:8585/api/v1/system/version", timeout=5)
                if response.status_code == 200:
                    print("✅ OpenMetadata iniciado com sucesso!")
                    return True
            except:
                pass
            time.sleep(2)
        
        print("❌ Timeout aguardando OpenMetadata")
        return False
        
    except subprocess.CalledProcessError as e:
        if "already in use" in str(e):
            print("⚠️  Container já existe. Tentando iniciar...")
            subprocess.run(["docker", "start", "openmetadata"])
            return True
        else:
            print(f"❌ Erro ao iniciar OpenMetadata: {e}")
            return False


def main():
    """Função principal"""
    print("🎯 Data Contracts Management com OpenMetadata")
    print("=" * 50)
    
    # Verificar Docker
    if not check_docker():
        print("❌ Docker não encontrado. Instale Docker primeiro:")
        print("   https://docs.docker.com/get-docker/")
        sys.exit(1)
    
    # Iniciar OpenMetadata
    if start_openmetadata():
        print("\n🎉 Tudo pronto!")
        print("📊 Acesse: http://localhost:8585")
        print("👤 Login: admin / admin")
        print("\n💡 Dicas:")
        print("   - Crie seus datasets na seção 'Tables'")
        print("   - Configure validações em 'Data Quality'")
        print("   - Veja linhagem em 'Lineage'")
    else:
        print("❌ Falha ao iniciar OpenMetadata")
        sys.exit(1)


if __name__ == "__main__":
    main()
