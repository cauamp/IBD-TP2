from dotenv import load_dotenv
import pandas as pd
import psycopg2
import re
import os
from io import StringIO

load_dotenv()
DATABASE = os.getenv("DATABASE_URL")

def connect_db():
    try:
        conn = psycopg2.connect(DATABASE)
        print("Conexão bem sucedida ao banco de dados PostgreSQL!")
        return conn
    except psycopg2.Error as e:
        print("Erro ao conectar ao banco de dados PostgreSQL:", e)
        return None

# Função para limpar e corrigir o XML
def limpar_corrigir_xml(conteudo):
    # Substituir ou remover caracteres inválidos
    conteudo = re.sub(r'[^\x09\x0A\x0D\x20-\x7F]', '', conteudo)
    return conteudo

# Função para ler arquivo CSV ou XML
def ler_arquivo(caminho_arquivo):
    try:
        if not os.path.exists(caminho_arquivo):
            print(f"Erro: O arquivo '{caminho_arquivo}' não foi encontrado.")
            return None

        if caminho_arquivo.endswith('.csv'):
            # Adicione o separador desejado aqui
            if 'SISU' in caminho_arquivo:
                tabela = pd.read_csv(caminho_arquivo, encoding='latin1', sep='|')
            elif 'Prouni' in caminho_arquivo:
                tabela = pd.read_csv(caminho_arquivo, encoding='latin1', sep=';')
            else:
                print("Erro: O nome do arquivo CSV não corresponde a SISU ou Prouni.")
                return None
        elif caminho_arquivo.endswith('.xml'):
            with open(caminho_arquivo, 'r', encoding='utf-8') as file:
                conteudo = file.read()
            conteudo_limpo = limpar_corrigir_xml(conteudo)
            # Adicionar uma tag raiz se necessário
            if not conteudo_limpo.strip().startswith("<root>"):
                conteudo_limpo = "<root>" + conteudo_limpo + "</root>"
            tabela = pd.read_xml(StringIO(conteudo_limpo), parser='etree')
        else:
            print("Formato de arquivo não suportado. Utilize .csv ou .xml.")
            return None
        tabela.drop(columns=['__parsed_extra'], inplace=True)
        print(tabela.head())
        return tabela
    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_arquivo}' não foi encontrado.")
    except pd.errors.EmptyDataError:
        print(f"Erro: O arquivo '{caminho_arquivo}' está vazio.")
    except pd.errors.ParserError:
        print(f"Erro: O arquivo '{caminho_arquivo}' está mal formatado.")
    except Exception as e:
        print(f"Erro: Ocorreu um erro ao ler o arquivo. Detalhes: {e}")
        return None

# Função para mapear tipos de dados do pandas para PostgreSQL
def mapear_tipo_pandas_para_postgresql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

# Função para criar a tabela no banco de dados
def criar_tabela(conn, tabela, nome_tabela):
    try:
        cur = conn.cursor()
        
        # Construir a instrução CREATE TABLE com base nos tipos de dados do DataFrame
        colunas = []
        for coluna, dtype in tabela.dtypes.items():
            coluna = re.sub(r'\W', '', coluna)  # Remover caracteres não alfanuméricos dos nomes das colunas
            tipo = mapear_tipo_pandas_para_postgresql(dtype)
            colunas.append(f'"{coluna}" {tipo}')
        colunas_str = ", ".join(colunas)
        
        # Executar a instrução CREATE TABLE
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{nome_tabela}" (
            {colunas_str}
        )
        """)
        conn.commit()
        cur.close()
        print(f"Tabela '{nome_tabela}' criada com sucesso!")
    except psycopg2.Error as e:
        conn.rollback()  # Reverter a transação em caso de erro
        print("Erro ao criar a tabela:", e)

def inserir_dados(conn, tabela, nome_tabela):
    try:
        cur = conn.cursor()
        
        # Criar um objeto StringIO para armazenar os dados formatados
        output = StringIO()
        tabela.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        # Utilizar copy_from para inserir dados em massa
        cur.copy_from(output, nome_tabela, null="", sep='\t')

        conn.commit()
        cur.close()
        print(f"Dados inseridos com sucesso na tabela '{nome_tabela}'!")
    except psycopg2.Error as e:
        conn.rollback()  # Reverter a transação em caso de erro
        print("Erro ao inserir dados:", e)

dict_arquivo_tabela = {
    'static/sdsdsd.xml': 'Relatorio_SISU_1_18',
    
}

conn = connect_db()
if conn is not None:
    for k, v in dict_arquivo_tabela.items():
        print(f"Processando o arquivo: {k}")
        tabela = ler_arquivo(k)
        if tabela is not None:
            criar_tabela(conn, tabela, v)
            print(f"Número total de linhas para inserir: {len(tabela)}")
            inserir_dados(conn, tabela, v)
    conn.close()
    print("Conexão com o banco de dados fechada.")