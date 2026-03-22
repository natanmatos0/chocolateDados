"""
Resumo:
1. Conexao com o datalake
2. Download de 4 arquivos
3. Converter pra DFs
4. Salvar no PostegreSQL


Fluxo completo de ingestão de dados
EXTRACTION ---> LOAD (EL)
Dados salvos extamente como os que vem do parquet
"""


# %%
import boto3 # biblioteca nescenssaria pra se comunicar com o AWS S3
import io # transforma arquivos em bytes e traz pra memoria
import pandas as pd # manipular dataframe
from sqlalchemy import create_engine # criar conexão com o DB
from dotenv import load_dotenv
import os

# %%
load_dotenv()
S3_ENPOINT_URL = os.environ.get("S3_ENPOINT_URL")
AWS_REGION = os.environ.get("AWS_REGION")
AWS_ACESS_KEY_ID = os.environ.get("AWS_ACESS_KEY_ID")
AWS_SECRET_ACESS_KEY = os.environ.get("AWS_SECRET_ACESS_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# %%
# configuração do acesso ao datalake
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    endpoint_url=S3_ENPOINT_URL,
    aws_access_key_id=AWS_ACESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACESS_KEY
)

# %%
# Listar os arquivos do bucket
response = s3.list_objects(Bucket=BUCKET_NAME)
arquivos = [obj["Key"] for obj in response["Contents"]]
print(arquivos)

# %%
# Ler as tabelas do datalake
TABELAS = ["calendar", "customers", "products", "sales", "stores"]

# Dicionario vazio onde os DF's vao ser guradados
# Chave: Nome da tabela; Valor: DF com os dados
dataframes = {}

# %%
# FOR 1: percorre cada tabela e baixa o datalake
for tabela in TABELAS:
    print(f"baixando a tabela {tabela}.parquet do datalake")

    # Monta o nome do arquivo: xxxx ---> xxxx.parquet
    file_key = f"{tabela}.parquet"

    # Baixa o arquivo do s3
    response = s3.get_object(
      Bucket=BUCKET_NAME,
      Key=file_key
  )
    parquet_bytes = response["Body"].read()

    # Converte o arquivo para que possa ser lido
    dataframes[tabela] = pd.read_parquet(io.BytesIO(parquet_bytes))

    # Confirmação de download
    print(f"{tabela}: {len(dataframes[tabela])} linhas carregadas")

# %%
# FOR 2: percorre o dicionario e salva cada tabela no banco
for tabela, df in dataframes.items():
    print(f"Salvando {tabela} no PostgreSQL")

    # Manda o df como uma tabela pro banco
    df.to_sql(
        tabela,
        con=engine,
        if_exists="replace",
        index=False
    )
    print(f"{tabela}: {len(df)} linhas salvas no banco")

# %%
# FOR 3: Verifica se os dados foram enviados corretamente
for tabela in TABELAS:
    df_verificacao = pd.read_sql_query(f"SELECT COUNT(*) as total FROM {tabela}", engine)
    total = df_verificacao["total"].iloc[0]
    print(f"{tabela}: {total} linhas no banco")
engine.dispose()