#!/usr/bin/env python3
"""
Script principal para criar dados de exemplo e validar com Great Expectations
"""
import pandas as pd
import great_expectations as gx
from pathlib import Path


def create_sample_data():
    """Cria dados de exemplo para testar"""
    print("📊 Criando dados de exemplo...")
    
    # Criar diretório data se não existir
    Path("data").mkdir(exist_ok=True)
    
    # Dados de exemplo - eventos de usuário
    sample_data = {
        "user_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "event_type": ["login", "click", "view", "purchase", "logout", "login", "click", "view", "purchase", "logout"],
        "timestamp": [
            "2024-01-01 10:00:00", "2024-01-01 10:05:00", "2024-01-01 10:10:00",
            "2024-01-01 10:15:00", "2024-01-01 10:20:00", "2024-01-01 11:00:00",
            "2024-01-01 11:05:00", "2024-01-01 11:10:00", "2024-01-01 11:15:00",
            "2024-01-01 11:20:00"
        ],
        "amount": [None, None, None, 99.99, None, None, None, None, 149.99, None]
    }
    
    df = pd.DataFrame(sample_data)
    df.to_csv("data/user_events.csv", index=False)
    
    # Dados com problemas para testar validação
    bad_data = {
        "user_id": [1, 2, None, 4, -1],  # user_id nulo e negativo
        "event_type": ["login", "invalid_event", "view", "purchase", "logout"],  # evento inválido
        "timestamp": [
            "2024-01-01 10:00:00", "invalid_date", "2024-01-01 10:10:00",
            "2024-01-01 10:15:00", "2024-01-01 10:20:00"
        ],
        "amount": [None, None, None, -50.0, None]  # valor negativo
    }
    
    df_bad = pd.DataFrame(bad_data)
    df_bad.to_csv("data/user_events_bad.csv", index=False)
    
    print("✅ Arquivos criados:")
    print("   - data/user_events.csv (dados válidos)")
    print("   - data/user_events_bad.csv (dados com problemas)")


def setup_great_expectations():
    """Configura Great Expectations se ainda não foi configurado"""
    print("🔧 Configurando Great Expectations...")
    
    try:
        # Tentar obter contexto existente
        context = gx.get_context()
        print("✅ Great Expectations já configurado")
    except:
        # Se não existir, inicializar
        print("🆕 Inicializando Great Expectations...")
        context = gx.get_context(mode="file")
    
    return context


def create_expectations():
    """Cria expectativas para validação de dados"""
    print("📋 Criando expectativas de validação...")
    
    context = setup_great_expectations()
    
    # Criar suite de expectativas
    suite_name = "user_events_suite"
    
    try:
        suite = context.add_expectation_suite(suite_name)
    except:
        # Suite já existe, recuperar e limpar
        suite = context.get_expectation_suite(suite_name)
        suite.expectations = []
    
    # Adicionar expectativas
    expectations = [
        # Colunas obrigatórias
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "user_id"}},
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "event_type"}},
        {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "timestamp"}},
        
        # user_id não pode ser nulo e deve ser positivo
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "user_id"}},
        {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "user_id", "min_value": 1}},
        
        # event_type deve estar na lista de valores válidos
        {"expectation_type": "expect_column_values_to_be_in_set", 
         "kwargs": {"column": "event_type", "value_set": ["login", "logout", "click", "view", "purchase"]}},
        
        # timestamp não pode ser nulo
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "timestamp"}},
        
        # amount deve ser positivo quando não for nulo
        {"expectation_type": "expect_column_values_to_be_between", 
         "kwargs": {"column": "amount", "min_value": 0, "mostly": 1.0}}
    ]
    
    for exp_config in expectations:
        suite.add_expectation(gx.expectations.ExpectationConfiguration(**exp_config))
    
    context.save_expectation_suite(suite)
    print(f"✅ Suite '{suite_name}' criada com {len(expectations)} expectativas")


def validate_data():
    """Valida os dados usando as expectativas"""
    print("🔍 Validando dados...")
    
    context = setup_great_expectations()
    
    # Validar dados bons
    print("\n📊 Validando user_events.csv (dados válidos):")
    df_good = pd.read_csv("data/user_events.csv")
    
    validator = context.get_validator(
        batch_request=context.sources.pandas_default.read_dataframe(df_good),
        expectation_suite_name="user_events_suite"
    )
    
    results_good = validator.validate()
    print(f"✅ Resultado: {'PASSOU' if results_good.success else 'FALHOU'}")
    
    # Validar dados ruins
    print("\n📊 Validando user_events_bad.csv (dados com problemas):")
    df_bad = pd.read_csv("data/user_events_bad.csv")
    
    validator_bad = context.get_validator(
        batch_request=context.sources.pandas_default.read_dataframe(df_bad),
        expectation_suite_name="user_events_suite"
    )
    
    results_bad = validator_bad.validate()
    print(f"❌ Resultado: {'PASSOU' if results_bad.success else 'FALHOU (esperado)'}")
    
    # Mostrar falhas
    failed_expectations = [r for r in results_bad.results if not r.success]
    if failed_expectations:
        print(f"\n🔍 Expectativas que falharam ({len(failed_expectations)}):")
        for result in failed_expectations[:3]:  # Mostrar apenas as primeiras 3
            print(f"   - {result.expectation_config.expectation_type}")


def main():
    """Função principal"""
    print("🎯 Data Contracts Management - Great Expectations")
    print("=" * 50)
    
    # Criar dados de exemplo
    create_sample_data()
    
    # Configurar Great Expectations
    setup_great_expectations()
    
    # Criar expectativas
    create_expectations()
    
    # Validar dados
    validate_data()
    
    print("\n🎉 Tudo pronto!")
    print("\n📋 Próximos passos:")
    print("1. Execute: uv run great_expectations docs build --site-name local")
    print("2. Acesse a UI em: http://localhost:8080")
    print("3. Explore os resultados de validação!")


if __name__ == "__main__":
    main()
