# Instalação (se ainda não tiver instalado)
# pip install sqlalchemy pymysql pandas

from sqlalchemy import create_engine, text
import pandas as pd

# Configurações do banco
host = 'localhost'
user = 'root'
password = ''
database = 'bd_livraria'


# Função para conectar e buscar tabelas do MySQL
def busca_dados_sql(tabela):
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        with engine.connect() as conexao:
            query = f"SELECT * FROM {tabela}"
            df = pd.read_sql(text(query), conexao)
            return df
    except Exception as e:
        print(f'Erro ao conectar ao banco: {e}')
        return pd.DataFrame()  # Retorna DataFrame vazio em caso de erro


# Lê os dados SQL
df_livros = busca_dados_sql('tb_livros')
df_usuarios = busca_dados_sql('tb_usuarios')


# Lê os dados dos aquivos CSV (importados externamente)
# Se for arquivo do excel: pd.read_excel()
df_emprestimos = pd.read_csv('tb_emprestimos.csv')  
df_itens = pd.read_csv('tb_itens_emprestados.csv', sep='\t')

# Relaciona os dados no Python
# Uso do Merge - É o nosso Join
df_completo = pd.merge(df_itens, df_emprestimos, on='id_emprestimo')
df_completo = pd.merge(df_completo, df_usuarios, on='id_usuario')
df_completo = pd.merge(df_completo, df_livros, on='id_livro')

# Exibe uma visão final com nome do usuário, data e valor total dos livros
df_resumo = df_completo.groupby(['nome', 'data_emprestimo'])['valor_emprestimo'].sum().reset_index()
df_resumo.rename(columns={
    'nome': 'nome_usuario',
    'valor_emprestimo': 'total_emprestado'
}, inplace=True)

print(df_resumo)