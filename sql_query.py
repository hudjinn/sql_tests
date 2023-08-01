import psycopg2
import pandas as pd
import numpy as np
class UserAuth:
    user = ''
    password = ''

def db_connect(user, password):
    conn = psycopg2.connect(database="func", user=user, password=password, host="", port="5432")
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
    lista de estacoes
    stations_sql = "SELECT DISTINCT(dse_estacao) FROM pcd.dado_sensor"
    stns = select(stations_sql)

    #stns = pd.read_csv("stn_list.csv")
    #stns = stns['dse_estacao'].to_list()
    #stns = [12,35]
    #stns.to_csv("stn_list.csv", index=False)
    
    final_df = pd.DataFrame()

    for stn in stns:
        metadata_sql = f"SELECT est_codigo, LEFT(CAST(est_municipio AS VARCHAR), 2) as uf \
                    FROM pcd.estacao WHERE est_codigo = {stn}"
                 
        data_sql = f"SELECT dse_estacao, dse_sensor, MIN(dse_data) as data_inicial, MAX(dse_data) as data_final, count(*) as qnt_dados \
                    FROM pcd.dado_sensor WHERE dse_estacao = {stn}\
                    GROUP BY dse_sensor, dse_estacao \
                    ORDER BY dse_estacao ASC"
        metadata_df = select(metadata_sql)
        data_df = select(data_sql)
        
        if metadata_df.empty:
            data_df['uf'] = np.nan
        else:
            data_df['uf'] = metadata_df['uf'].values[0]
            #print(metadata_df['uf'].values[0])
        final_df = pd.concat([final_df, data_df], ignore_index=True)
        print(final_df)
    print(20*"=", "\n", final_df)
    final_df.to_csv("output_dse.csv", index=False)
