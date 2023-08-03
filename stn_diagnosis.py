#%%
#coding: utf-8
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.cm import Paired  # type: ignore
from IPython.display import display

raw_data_df = pd.read_csv("stn_diagnosis.csv", index_col=["dse_estacao", "dse_sensor"])
raw_data_df = raw_data_df.drop(columns=['est_ins_codigo', 'est_nome', 'uf'])

metadata_df = pd.read_csv("/home/jefferson.galvao/Documents/projects/ray_x_bd/stn_metadata.csv", index_col="est_codigo")
metadata_df.index.names = ["dse_estacao"]

raw_data_df = pd.merge(raw_data_df.reset_index(), metadata_df.reset_index(), on=["dse_estacao"], how='outer').set_index(["dse_estacao", "dse_sensor"])

ins_df = pd.read_csv("data-1690897886740.csv")


#%%  
#INSERINDO BOOLEANOS INDICATIVOS

raw_data_df['dado_sensor'] = ~raw_data_df['data_inicial'].isna()
raw_data_df['estacao'] = ~raw_data_df['est_ins_codigo'].isna()



#%%
#FILTRAGEM
#Estacoes que so existem na pcd.dado_sensor
dado_sensor_only = raw_data_df[(raw_data_df['dado_sensor']== True) & (raw_data_df['estacao']== False)]
print(40*"=", "\033[91m\nESTAÇÕES COM DADOS APENAS NA PCD.DADO_SENSOR\033[0m\n", 40*"=","\n",dado_sensor_only)
dado_sensor_only.to_csv("outputs/tabelas/estacoes_com_dados_apenas_na_dado_sensor.csv")

#%%
#Estações que só existem na pcd.estacao
estacao_only = raw_data_df[(raw_data_df['estacao']== True) & (raw_data_df['dado_sensor']== False)]
print(40*"=", "\033[91m\nESTAÇÕES COM DADOS APENAS NA PCD.ESTACAO\033[0m\n", 40*"=","\n",estacao_only)
estacao_only.to_csv("outputs/tabelas/estacoes_com_dados_apenas_na_estacao.csv")

#%%
#Estações com menos de 120 dias de dados
#corrigindo colunas de data
fix_data_df = raw_data_df

fix_data_df["data_inicial"] = pd.to_datetime(fix_data_df['data_inicial'], errors='coerce')
fix_data_df["data_final"] = pd.to_datetime(fix_data_df['data_final'], errors='coerce')

#Estacao com erro em data
erro_data_df = fix_data_df[(fix_data_df["data_inicial"].isna()) & (fix_data_df["dado_sensor"] == True)]
print(40*"=", "\033[91m\nESTAÇÕES COM ERRO NA DATA.ESTACAO\033[0m\n", 40*"=","\n",erro_data_df)
erro_data_df.to_csv("outputs/tabelas/estacoes_com_erro_na_data.csv")

#%%
#Calcular a diferença em dias
fix_data_df["diferenca_dias"] = (fix_data_df["data_final"] - fix_data_df["data_inicial"]).dt.days

#Filtrar as linhas onde a diferença é menor que 120 dias
lessThen120days = fix_data_df[fix_data_df["diferenca_dias"] < 120]
print(60*"=", "\033[91m\nESTAÇÕES COM INTERVALO DE DATAS COM MENOS DE 120 DIAS\033[0m\n", 60*"=","\n",lessThen120days)
lessThen120days.to_csv("outputs/tabelas/estacoes_com_menos_de_120_dias_de_dados.csv")


# Filtrar as linhas onde a diferença é menor que 120 dias e o ano da data final não é 2023
lessThen120days_filtered = lessThen120days[
    (lessThen120days["diferenca_dias"] < 120) & (lessThen120days["data_final"].dt.year != 2023)
]

print(60*"=", "\033[91m\nESTAÇÕES COM INTERVALO DE DATAS COM MENOS DE 120 DIAS E NÃO DE 2023\033[0m\n", 60*"=", "\n", lessThen120days_filtered)
lessThen120days_filtered.to_csv("outputs/tabelas/estacoes_com_menos_de_120_dias_nao_2023.csv")
#%%
#Total Estacoes
totalEstacoes = raw_data_df.index.get_level_values('dse_estacao').nunique()

#Apenas em pcd.estacao
estacaoCount = estacao_only.index.get_level_values('dse_estacao').nunique()

#Apenas em pcd.dado_sensor
dadoSensorCount = dado_sensor_only.index.get_level_values('dse_estacao').nunique()

print(f"Total de Estações: {totalEstacoes}\nApenas em pcd.estacao: {estacaoCount}\nApenas em pcd.dado_sensor: {dadoSensorCount}")
#%%
#Estacoes com poucos dados
less120daysCount = lessThen120days.index.get_level_values('dse_estacao').nunique()

#Estacoes com erros grosseiros na data
erroDataCount = erro_data_df.index.get_level_values('dse_estacao').nunique()

print(f"Estações com menos de 120 dias de intervalo de dados: {less120daysCount}\nEstações com erros nas datas: {erroDataCount}")

#%%
#Gerar Gráficos

categories = ['Total de Estações', 'Apenas em pcd.estacao', 'Estações com menos de 120 dias','Apenas em pcd.dado_sensor',  'Estações com erros nas datas']
values = [totalEstacoes, estacaoCount, less120daysCount, dadoSensorCount, erroDataCount]

# Criando o gráfico de barras empilhadas
plt.figure(figsize=(10, 6))
bars = plt.bar(categories, values, color=['blue', 'orange', 'green', 'red', 'purple'])

# Adicionando rótulos nas barras
for bar, value in zip(bars, values):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(value), ha='center', va='bottom')

plt.title('Dados de Estações nas tabelas pcd.estacao e pcd.dado_sensor')
plt.xlabel('Categorias')
plt.ylabel('Contagem')
plt.xticks(rotation=15, ha='right')
plt.tight_layout()
plt.show()

#%%
#Grafico 2 - Pizza por instituição
#criar dicionario de codigo de instituições

# Agrupando dados por instituição e contando quantidades
grouped_by_instituicao = lessThen120days.groupby('est_ins_codigo').size()
grouped_by_instituicao.index = grouped_by_instituicao.index.astype(int)

# Organizando em ordem decrescente
sorted_grouped = grouped_by_instituicao.sort_values(ascending=False)
ins_dict = dict(zip(ins_df['est_ins_codigo'], ins_df['ins_nome']))
# Substituindo os códigos de instituição pelos nomes de instituição no índice do DataFrame
sorted_grouped.index = sorted_grouped.index.map(ins_dict)

# Criando o gráfico de barras
plt.figure(figsize=(10, 6))
colors = Paired.colors
ax = sorted_grouped.plot(kind='bar', color=colors)
plt.title('Estações com menos de 120 dias de dados (por Instituição)')
plt.xlabel('Instituição')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')

# Adicionando as quantidades nas barras
for i, v in enumerate(sorted_grouped):
    ax.text(i, v + 1, str(v), color='black', ha='center', va='bottom')

plt.tight_layout()
plt.show()
#%%
#Grafico 3 
# Criando um DataFrame para contar as instituições que estão apenas em pcd.estacao
instituicoes_apenas_estacao = estacao_only.groupby('est_ins_codigo').size()
instituicoes_apenas_estacao.index = instituicoes_apenas_estacao.index.astype(int)
# Substituindo os códigos de instituição pelos nomes de instituição no índice do DataFrame, ou mantendo o código se não houver correspondência
instituicoes_apenas_estacao.index = instituicoes_apenas_estacao.index.map(lambda x: ins_dict.get(x, x))

# Organizando em ordem decrescente
sorted_instituicoes_apenas_estacao = instituicoes_apenas_estacao.sort_values(ascending=False)

# Criando o gráfico de barras
plt.figure(figsize=(10, 6))
colors = Paired.colors
ax = sorted_instituicoes_apenas_estacao.plot(kind='bar', color=colors)
plt.title('Instituições com dados apenas em pcd.estacao')
plt.xlabel('Instituição')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')

# Adicionando as quantidades nas barras
for i, v in enumerate(sorted_instituicoes_apenas_estacao):
    ax.text(i, v + 1, str(v), color='black', ha='center', va='bottom')

plt.tight_layout()
plt.show()

#%%
# Agrupando dados por instituição e contando quantidades para a nova filtragem
grouped_by_instituicao_filtered = lessThen120days_filtered.groupby('est_ins_codigo').size()
grouped_by_instituicao_filtered.index = grouped_by_instituicao_filtered.index.astype(int)

# Organizando em ordem decrescente
sorted_grouped_filtered = grouped_by_instituicao_filtered.sort_values(ascending=False)

# Substituindo os códigos de instituição pelos nomes de instituição no índice do DataFrame
sorted_grouped_filtered.index = sorted_grouped_filtered.index.map(ins_dict)

# Criando o gráfico de barras
plt.figure(figsize=(10, 6))
colors = Paired.colors
ax = sorted_grouped_filtered.plot(kind='bar', color=colors)
plt.title('Estações com menos de 120 dias de dados (não em 2023) por Instituição')
plt.xlabel('Instituição')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')

# Adicionando as quantidades nas barras
for i, v in enumerate(sorted_grouped_filtered):
    ax.text(i, v + 1, str(v), color='black', ha='center', va='bottom')

plt.tight_layout()
plt.show()

#%%
# Filtrar o DataFrame com base no critério
filtro_modelo_1 = (raw_data_df['est_modelo'] == 1.0) & (raw_data_df['dado_sensor'] == False)
filtered_df = raw_data_df[filtro_modelo_1]

# Agrupar dados por instituição e contar quantidades
grouped_by_instituicao_filtered = filtered_df.groupby('est_ins_codigo').size()
grouped_by_instituicao_filtered.index = grouped_by_instituicao_filtered.index.astype(int)

# Organizar em ordem decrescente
sorted_grouped_filtered = grouped_by_instituicao_filtered.sort_values(ascending=False)

# Substituir os códigos de instituição pelos nomes de instituição no índice do DataFrame
sorted_grouped_filtered.index = sorted_grouped_filtered.index.map(ins_dict)

# Criar o gráfico de barras
plt.figure(figsize=(10, 6))
colors = Paired.colors
ax = sorted_grouped_filtered.plot(kind='bar', color=colors)
plt.title('Estações com modelo 1.0 e apenas em pcd.estacao')
plt.xlabel('Instituição')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')

# Adicionar as quantidades nas barras
for i, v in enumerate(sorted_grouped_filtered):
    ax.text(i, v + 1, str(v), color='black', ha='center', va='bottom')

plt.tight_layout()
plt.show()

#%%
# Filtrar o DataFrame com base no critério
filtro_modelo_1 = (raw_data_df['est_modelo'] == 2.0) & (raw_data_df['dado_sensor'] == False)
filtered_df = raw_data_df[filtro_modelo_1]

# Agrupar dados por instituição e contar quantidades
grouped_by_instituicao_filtered = filtered_df.groupby('est_ins_codigo').size()
grouped_by_instituicao_filtered.index = grouped_by_instituicao_filtered.index.astype(int)

# Organizar em ordem decrescente
sorted_grouped_filtered = grouped_by_instituicao_filtered.sort_values(ascending=False)

# Substituir os códigos de instituição pelos nomes de instituição no índice do DataFrame
sorted_grouped_filtered.index = sorted_grouped_filtered.index.map(ins_dict)

# Criar o gráfico de barras
plt.figure(figsize=(10, 6))
colors = Paired.colors
ax = sorted_grouped_filtered.plot(kind='bar', color=colors)
plt.title('Estações com modelo 2.0 e apenas em pcd.estacao')
plt.xlabel('Instituição')
plt.ylabel('Contagem')
plt.xticks(rotation=45, ha='right')

# Adicionar as quantidades nas barras
for i, v in enumerate(sorted_grouped_filtered):
    ax.text(i, v + 1, str(v), color='black', ha='center', va='bottom')

plt.tight_layout()
plt.show()
