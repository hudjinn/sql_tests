import pandas as pd
import ast
df = pd.read_csv('resultado_teste_2.csv')

basicfilter = (df['status'] == 'ERRO')
df_filtered = df[basicfilter]

with open('log.txt', "w") as log:

    #FUNCAO DELETE
    delete_filter = (df_filtered['sensors_with_data'] == '[]')
    df_to_delete = df_filtered[delete_filter]

    stn_to_delete = df_to_delete['est_codigo'].to_list()

    with open('stn_to_delete_sql.txt', "w") as arquivo:
        text = f'DELETE FROM pcd.estacao_sensor WHERE ess_estacao IN {tuple(stn_to_delete)};'
        arquivo.write(text)
        log.write(text)

    #SEM CADASTRO DE SENSOR
    ess_filter = (df_filtered['registered_sensors'] == '[]')
    ess_df = df_filtered[ess_filter]
    print(ess_df)
    with open('stn_to_insert_sql.txt', "w") as arquivo:
        for index, row in ess_df.iterrows():
            estacao = row['est_codigo']
            sensors = row['sensors_with_data']
            literalsensors = ast.literal_eval(sensors)
            for sensor in literalsensors:
                text = f"INSERT INTO pcd.estacao_sensor (ess_estacao, ess_sensor) VALUES ({estacao}, {sensor});\n"
                arquivo.write(text)
                log.write(text)
    #DIFERENÇA NO CADASTRO
    dif = (df_filtered['sensors_with_data'] != "[]")
    df_dif = df_filtered[dif]

    with open('stn_update_delete_sql.txt', "w") as arquivo1:
        with open('stn_update_sql.txt', "w") as arquivo2:
            for index, row in df_dif.iterrows():
                estacao = row['est_codigo']
                sensors = row['sensors_with_data']
                ess_sensors = row['registered_sensors']
                literal_sensors_with_data = set(ast.literal_eval(sensors))
                literal_estacao_sensors = set(ast.literal_eval(ess_sensors))
                log.write(str(literal_sensors_with_data))
                log.write(str(literal_estacao_sensors))
                to_insert = literal_sensors_with_data.difference(literal_estacao_sensors)
                log.write(str(to_insert))
                if to_insert == set():
                    pass
                else:
                    
                        for sensor in to_insert:
                            text = f"INSERT INTO pcd.estacao_sensor (ess_estacao, ess_sensor) VALUES ({estacao}, {sensor});\n"
                            
                            arquivo2.write(text)
                            log.write(text)
                to_delete = literal_estacao_sensors.difference(literal_sensors_with_data)
                if to_delete == set():
                    pass
                else:
                    
                        for sensor in to_delete:
                            text = f"DELETE FROM pcd.estacao_sensor WHERE ess_estacao = {estacao} AND ess_sensor = {sensor};\n"
                            log.write(text)

                            arquivo1.write(text)
                        
