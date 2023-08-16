import psycopg2
import pandas as pd
import numpy as np
import requests as req
import re
import matplotlib.pyplot as plt

class UserAuth:
    user = 'jefferson_galvao'
    password = ''

def db_connect(user, password):
    conn = psycopg2.connect(database="func", user=user, password=password, host="192.168.3.41", port="5432")
    conn.autocommit = True
    return conn

def select(sql):
    auth = UserAuth
    with db_connect(auth.user, auth.password) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            register = cur.fetchall()
            columns = cur.description
            columns_res = [{columns[index][0]: column for index, column in enumerate(value)} for value in register]
            data_result = pd.DataFrame(data=columns_res)
    return data_result

if __name__ == "__main__":
    url = 'web/storage/obs/monitor_secas/pcds_data/raw/2023-07/daily/t2m/CE'
    response = req.get(url)
    content = response.text

    file_names = re.findall(r'-CE_t2m_(\d+)\.txt', content)
    url_names = re.findall(r'href="(.*?)_t2m', content)
    numeration_list = [int(number) for number in file_names]
    unique_numeration_list = list(set(numeration_list))

    station_url_dict = {station: name for station, name in zip(unique_numeration_list, url_names)}

    for stn in unique_numeration_list:
        data_sql = f"SELECT estacao_id, sensor_id, data, valor_bruto \
                    FROM pcd.dado_sensor_diario \
                    WHERE estacao_id = {stn} AND sensor_id = 4 and data > CURRENT_DATE - INTERVAL '180 days' \
                    order by data asc;"

        data_df = select(data_sql)
        if not data_df.empty:
            data_df['valor_bruto'] = data_df['valor_bruto'].apply(lambda x: float(x)).round(1)

            monitor_url = f'web/storage/obs/monitor_secas/pcds_data/raw/2023-07/daily/t2mmin/CE/{station_url_dict[stn]}_t2mmin_{stn}.txt'

            df_monitor = pd.DataFrame()
            try:
                df_header = pd.read_csv(monitor_url, nrows=6,
                                        header=None, sep=':').set_index([0])

                df_monitor = pd.read_csv(monitor_url, delim_whitespace=True,
                                    skiprows=6, header=None,
                                    parse_dates={'datetime': [0, 1, 2]}).set_index('datetime')
            except:
                pass
            
            if df_monitor.empty:
                pass
            else:
                df_monitor.columns = ['valor_monitor']
                df_monitor['valor_monitor'] = df_monitor['valor_monitor'].astype(float)
                df_monitor = df_monitor.reset_index()
                df_monitor = df_monitor.rename(columns={'datetime':'data'})
                data_inicio = pd.to_datetime('today') - pd.Timedelta(days=180)
                df_monitor = df_monitor[df_monitor['data'] > data_inicio]
                
                merged_df = pd.merge(data_df, df_monitor, on='data', how='inner')
                if not merged_df.empty:
                    merged_df['dif'] = merged_df['valor_bruto'] - merged_df['valor_monitor']
                    print(merged_df)
                    x = np.linspace(-10, 10, 100)
                    plt.figure(figsize=(10, 6))
                    plt.bar(merged_df['data'], merged_df['dif'], color='blue')

                    # Definir os limites do eixo y
                    plt.ylim(-10, 10)

                    # Adicionar uma linha horizontal em y = 0
                    plt.axhline(y=0, color='red', linestyle='--')

                    # Adicionar rótulos e título
                    plt.xlabel('Data')
                    plt.ylabel('Diferença')
                    plt.title(f'Diferença entre Valores Brutos e Monitorados para Estação {stn}')

                    plt.savefig(f'outputs/graficos/verificacao_stn/{stn}.png')
                    plt.clf()

