from textwrap import indent
import sqlalchemy
import psycopg2 as psg
from environs import Env
import pandas as pd
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import insert

env = Env()
env.read_env()

class BancoSistransito:

    def __init__(self):
        self.__connection = env('SISTRANSITO_DATABASE')

    def __get_connection(self):
        engine = sqlalchemy.create_engine(self.__connection)
        return engine

    def def_temp_table(self):
        engine = self.__get_connection()
        self.connection = engine.connect()
        self.connection.execute('DROP TABLE IF EXISTS sistransito_temp')
        self.connection.execute('CREATE TEMPORARY TABLE sistransito_temp ( pk int NOT NULL, situacao_ocorrencia varchar NULL, uf_bop varchar NULL, nro_bop varchar NULL, data_registro_siac date NULL, hora_registro time NULL, data_fato_siac date NULL, dia_fato_siac_ref varchar NULL, mes_fato_siac_ref varchar NULL, ano_fato_siac_ref int4 NULL, mes_registro_siac_ref varchar NULL, mes_registro int4 NULL, mes_fato int4 NULL, ano_registro int4 NULL, ano_fato int4 NULL, dia_semana varchar NULL, hora_fato time NULL, faixa_hora varchar NULL, faixa_hora_2 varchar NULL, motivo_determinante varchar NULL, local_ocorrencia varchar NULL, local_ocorrencia_ref varchar NULL, regiao_siac_ref varchar NULL, risp_siac_ref varchar NULL, aisp_siac_ref varchar NULL, bairros_siac_ref varchar NULL, distritos varchar NULL, bairro_ocorrencia varchar NULL, endereco_ocorrência varchar NULL, meio_empregado varchar NULL, placa_veiculo varchar NULL, uf_veiculo varchar NULL, chassi_veiculo varchar NULL, marca varchar NULL, modelo varchar NULL, cor varchar NULL, tipo_veiculo varchar NULL, tipo_veiculo_siac_1 varchar NULL, tipo_veiculo_siac_2 varchar NULL, categoria varchar NULL, municipio_veiculo varchar NULL, informante varchar NULL, ano_fabricacao int4 NULL, ano_modelo int4 NULL, nome_portador varchar NULL, CONSTRAINT sistransito_pkey PRIMARY KEY (pk) );')

    def set_temp_data(self, df):
        df.to_sql("sistransito_temp",self.connection, index=False,if_exists="append")

    def merge_sistransito(self):
        self.connection.execute("""insert into sistransito (situacao_ocorrencia , uf_bop , nro_bop , data_registro_siac , hora_registro , data_fato_siac , dia_fato_siac_ref , mes_fato_siac_ref , ano_fato_siac_ref , mes_registro_siac_ref , mes_registro , mes_fato , ano_registro , ano_fato , dia_semana , hora_fato , faixa_hora , faixa_hora_2 , motivo_determinante , local_ocorrencia , local_ocorrencia_ref , regiao_siac_ref , risp_siac_ref , aisp_siac_ref , bairros_siac_ref , distritos , bairro_ocorrencia , endereco_ocorrência , meio_empregado , placa_veiculo , uf_veiculo , chassi_veiculo , marca , modelo , cor , tipo_veiculo , tipo_veiculo_siac_1 , tipo_veiculo_siac_2 , categoria , municipio_veiculo , informante , ano_fabricacao , ano_modelo , nome_portador) select situacao_ocorrencia , uf_bop , nro_bop , data_registro_siac , hora_registro , data_fato_siac , dia_fato_siac_ref , mes_fato_siac_ref , ano_fato_siac_ref , mes_registro_siac_ref , mes_registro , mes_fato , ano_registro , ano_fato , dia_semana , hora_fato , faixa_hora , faixa_hora_2 , motivo_determinante , local_ocorrencia , local_ocorrencia_ref , regiao_siac_ref , risp_siac_ref , aisp_siac_ref , bairros_siac_ref , distritos , bairro_ocorrencia , endereco_ocorrência , meio_empregado , placa_veiculo , uf_veiculo , chassi_veiculo , marca , modelo , cor , tipo_veiculo , tipo_veiculo_siac_1 , tipo_veiculo_siac_2 , categoria , municipio_veiculo , informante , ano_fabricacao , ano_modelo , nome_portador from sistransito_temp""")
