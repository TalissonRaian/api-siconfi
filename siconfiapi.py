import requests
import pandas as pd
import os
import time
import pyodbc
from sqlalchemy import create_engine
# url_rgf = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rgf?an_exercicio=2024&in_periodicidade=Q&nr_periodo=1&co_tipo_demonstrativo=RGF&no_anexo={Anexo}&co_esfera=M&co_poder=E&id_ente={ente}"

#http://app-apidatalake-byepd4d9bsd6bpc3.brazilsouth-01.azurewebsites.net/ords/siconfi/tt/rgf?an_exercicio=2024&in_periodicidade=Q&nr_periodo=1&co_tipo_demonstrativo=RGF&no_anexo=RGF-Anexo+03&co_esfera=M&co_poder=E&id_ente=1100122


dados = []

# exercicio = [2024, 2025]
#Extrai por enquanto so os municipios de Ji-Paraná e Jaru
EnteCodes = [1100114, 1100122]

# EnteCodes = [1100015, 1100379, 1100403, 1100346, 1100023, 1100452, 1100031, 
#                  1100601, 1100049, 1100700, 1100809, 1100908, 1100056, 1100924, 
#                  1100064, 1100072, 1100080, 1100940, 1100098, 1101005, 1100106, 
#                  1101104, 1100130, 1101203, 1101302, 1101401, 
#                  1100148, 1100338, 1101435, 1100502, 1100155, 1101450, 1100189, 
#                  1101468, 1100205, 1100254, 1101476, 1100262, 1100288, 1100296, 
#                  1101484, 1101492, 1100320, 1101500, 1101559, 1101609, 1101708, 
#                  1101757, 1101807, 1100304]


AnexosRGF = ["RGF-Anexo 01", "RGF-Anexo 02", "RGF-Anexo 03", 
                 "RGF-Anexo 04", "RGF-Anexo 05", "RGF-Anexo 06"]


AnexosRREO = ["RREO-Anexo 01", "RREO-Anexo 02", "RREO-Anexo 03", "RREO-Anexo 04", 
                  "RREO-Anexo 04 - RGPS", "RREO-Anexo 04 - RPPS", "RREO-Anexo 04.0 - RGPS", 
                  "RREO-Anexo 04.1", "RREO-Anexo 04.2", "RREO-Anexo 04.3 - RGPS", 
                  "RREO-Anexo 05", "RREO-Anexo 06", "RREO-Anexo 07", "RREO-Anexo 09", 
                  "RREO-Anexo 10 - RGPS", "RREO-Anexo 10 - RPPS", "RREO-Anexo 11", 
                  "RREO-Anexo 13", "RREO-Anexo 14"]

bimestre = [1,2,3,4,5,6]

quadrimestre = [1,2,3]

params = {'limit': 5000}

#RGF
for ente in EnteCodes:
    for anexo in AnexosRGF:
      for quad in quadrimestre:
        url_rgf = f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rgf?an_exercicio=2024&in_periodicidade=Q&nr_periodo={quad}&co_tipo_demonstrativo=RGF&no_anexo={anexo}&co_esfera=M&co_poder=E&id_ente={ente}"  
        response = requests.get(url_rgf, params=params)
        page_data = response.json().get("items", [])
        dados.extend(page_data)


# RREO
for ente in EnteCodes:
    for anexo in AnexosRREO:  
      for bim in bimestre:
        url_rreo = f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo?an_exercicio=2024&nr_periodo={bim}&co_tipo_demonstrativo=RREO&no_anexo={anexo}&co_esfera=&id_ente={ente}"
        response = requests.get(url_rreo, params=params)
        page_data = response.json().get("items", [])
        dados.extend(page_data)


# Criar um DataFrame com os dados
if dados:
        df = pd.DataFrame(dados)
if all(col in df.columns for col in ['exercicio','periodo','cod_ibge','anexo','coluna','cod_conta','conta','valor']):
        dados_refinado = df[['exercicio','periodo','cod_ibge','anexo','coluna','cod_conta','conta','valor']]
       
dados = pd.DataFrame(dados)

dados.to_csv('DadosApi1.csv', index=False)


# Conexão ao banco de dados SQL Server
server = os.getenv('SERVER', '')  
database = os.getenv('DATABASE', '')  
username = os.getenv('USERNAME', '')  
password = os.getenv('PASSWORD', '')  
driver = 'ODBC Driver 17 for SQL Server'
# String de conexão para SQLAlchemy
conn_str = f"mssql+pyodbc://{server}:0000/{database}?driver=ODBC Driver 17 for SQL Server&trusted_connection=yes;"
engine = create_engine(conn_str)

connection= engine.connect()
dados_refinado.to_sql(
    'siconfi_dados',
    con=connection, 
    schema='dbo', 
    if_exists='append', 
    index=False
)

print(dados)
print("Dados inseridos com sucesso!")
