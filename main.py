import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from requests import Session
import time
from timeit import default_timer as timer
import duckdb
import pandas as pd
import os
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
import time



intervalo_salvar = 10000
# --------------------------------------------------Bate zap--------------------------------------------------

contagem = 0

# Carregar os números de telefone do arquivo CSV para um DataFrame
tels = pd.read_csv(r"G:\Drives compartilhados\Mystery Machine\00. Suporte\06. Modelo WhatsApp.csv", sep=";", encoding="iso-8859-1")

# Definir o URL base e o cabeçalho da API
base_url = 'http://uppart.mine.nu:8700'
headers = {
    "accept": "*/*",
    "x-api-key": "9a1df664-6b7e-4e97-b8c2-148db724ad0b",
    "Content-Type": "application/json"
}

# IDs das sessões disponíveis

nums = ['1017', '7167', '0729']

# , '0095', '1755', '1590'

# Função para salvar os resultados parciais em um arquivo CSV
def salvar_resultados_parciais(resultados, nome_arquivo):
    resultados.to_csv(nome_arquivo, sep=';', index=False)


# Função para carregar os resultados parciais de um arquivo CSV
def carregar_resultados_parciais(nome_arquivo):
    try:
        return pd.read_csv(nome_arquivo, sep=';')
    except FileNotFoundError:
        return None

# Função para enviar requisições POST
def check_phone_status(phone_number, num, session):
    data = {"number": phone_number}
    session_url = f'{base_url}/client/isRegisteredUser/{num}'
    response = session.post(session_url, json=data, headers=headers, timeout=120)
    if response.status_code == 200:
        return (phone_number, response.json()['result'], num)
    else:
        return (phone_number, f"Error: {response.text}", num)

# Configurar ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=len(nums)) as executor, Session() as session:
    start = timer()
    futures = []

    for i, (index, row) in enumerate(tels.iterrows()):
        # Obtem o ID da sessão atual de forma cíclica
        num = nums[i % len(nums)]
        # Submete a tarefa para o executor
        future = executor.submit(check_phone_status, str(row['TEL']), num, session)
        futures.append(future)
        # Salvar os resultados parciais a cada intervalo definido
        if (i + 1) % intervalo_salvar == 0:
            resultados_parciais = pd.DataFrame([f.result() for f in futures if f.done()], columns=['phone_number', 'status', 'session_num'])
            salvar_resultados_parciais(resultados_parciais, 'resultados_parciais.csv')

    # Processar os resultados conforme eles vão ficando prontos
    for future in futures:
        phone_number, status, session_num = future.result()
        contagem += 1
        print(f"\nNúmero de telefone {phone_number}: {status}")
        print(f"na seção {session_num}")
        print(contagem)
        # time.sleep(0.2)

resultados_finais = pd.DataFrame([f.result() for f in futures if f.done()], columns=['phone_number', 'status', 'session_num'])
salvar_resultados_parciais(resultados_finais, 'resultados_finais.csv')

def bool_case(v):
                match str(v):
                    case 'True': return 'SIM'
                    case 'False': return 'NAO'
                    case _: return v
end = timer()
print("Total execution time:", timedelta(seconds=end - start))

results = [future.result() for future in futures if future.done()]
df = pd.DataFrame(results, columns=['phone_number', 'status', 'session_num'])

df.to_csv(r"G:\Meu Drive\Arquivos\Temporários\ZLulunanto.csv", sep=';', index=False)

# --------------------------------------------------Prepara arquivo--------------------------------------------------

hoje = pd.Timestamp.today()
cz = pd.DataFrame()
cz['CPF'] = tels['CPF']
cz['NOME'] = tels['NOME']
cz['DATA_NASCIMENTO'] = pd.to_datetime(tels['DATA_NASCIMENTO'], dayfirst=True)
cz['IDADE'] = tels['IDADE']
cz['DDD'] = df['phone_number'].astype('string').str[:2]
cz['TEL'] = df['phone_number'].astype('string').str[2:]
cz['DDD+TEL'] = df['phone_number']
cz['DDI+DDD+TEL'] = '55' + df['phone_number'].astype('string')
cz['FLAG'] = df['status'].apply(bool_case)
cz['HIGIENIZADO'] =  hoje.strftime('01/%m/%Y')

# cz.to_csv(r"G:\Drives compartilhados\Mystery Machine\00. Suporte\01. Higienizador\Arquivo Retornado.csv", sep=';', index=False)
cz.to_csv(r"G:\Drives compartilhados\Mystery Machine\00. Suporte\01. Higienizador\Arquivo Retornado.csv", sep=';', index=False)

# --------------------------------------------------Carrega a base--------------------------------------------------

duckdb.query(r"""
        copy(
            SELECT
                *
            from
                'G:\Drives compartilhados\Mystery Machine\00. Suporte\01. Higienizador\WhatsApp ( Higienizados ).parquet'
            ) TO 'G:\Drives compartilhados\Mystery Machine\00. Suporte\01. Higienizador\WhatsApp ( Higienizados ).csv' (delimiter ';')
        """)

base_concat = pd.read_csv(r"G:\Drives compartilhados\Mystery Machine\00. Suporte\01. Higienizador\WhatsApp ( Higienizados ).csv", sep=";")
base_concat['DATA_NASCIMENTO'] = pd.to_datetime(base_concat['DATA_NASCIMENTO'], dayfirst=True)
base_concat['DATA_NASCIMENTO'] = base_concat['DATA_NASCIMENTO'].dt.strftime('%d/%m/%Y')
cz['DATA_NASCIMENTO'] = pd.to_datetime(cz['DATA_NASCIMENTO'], dayfirst=True)
cz['DATA_NASCIMENTO'] = cz['DATA_NASCIMENTO'].dt.strftime('%d/%m/%Y')

base_nova = duckdb.sql("""
                    select
                        CPF::bigint as CPF
                        , NOME::STRING as NOME
                        , DATA_NASCIMENTO::STRING as DATA_NASCIMENTO
                        , IDADE::bigint AS IDADE
                        , DDD::bigint AS DDD
                        , TEL::bigint AS TEL
                        , FLAG::STRING AS FLAG
                        , HIGIENIZADO::STRING AS HIGIENIZADO
                    from
                       cz
                    where
                       FLAG = 'SIM'
""")

print(base_nova)
base_nova_df = base_nova.df()

df_combined = pd.concat([base_concat, base_nova_df], ignore_index=True)
df_combined.to_parquet(r"G:\Drives compartilhados\Mystery Machine\00. Suporte\01. Higienizador\WhatsApp ( Higienizados ).parquet")
print("Base concatenada")
