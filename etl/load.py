import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert


def load_data_ips(dim_ips: DataFrame, etl_conn):
    dim_ips.to_sql('dim_ips', etl_conn, if_exists='append', index_label='key_dim_ips')


def load_data_medico(dim_medico: DataFrame, etl_conn: Engine):
    dim_medico.to_sql('dim_medico', etl_conn, if_exists='append', index_label='key_dim_medico')


def load_data_persona(dim_persona: DataFrame, etl_conn: Engine):
    dim_persona.to_sql('dim_persona', con=etl_conn, index_label='key_dim_persona', if_exists='append')


def load_data_servicio(dim_servicio: DataFrame, etl_conn: Engine):
    dim_servicio.to_sql('dim_servicio', etl_conn, if_exists='append', index_label='key_dim_servicio')


def load_data_fecha(dim_fecha: DataFrame, etl_conn: Engine):
    dim_fecha.to_sql('dim_fecha', etl_conn, if_exists='append', index_label='key_dim_fecha')


def load_data_trans_servicio(trans_servicio: DataFrame, etl_conn: Engine):
    trans_servicio.to_sql('trans_servicio', etl_conn, if_exists='append', index_label='key_trans_servicio')


def load_hecho_atencion(hecho_atencion: DataFrame, etl_conn: Engine):
    hecho_atencion.to_sql('hecho_atencion', etl_conn, if_exists='append', index=False)


def load(table: DataFrame, etl_conn: Engine, tname):
    # statement = insert(f'{table})
    # with etl_conn.connect() as conn:
    #     conn.execute(statement)
    table.to_sql(f'{tname}', etl_conn, if_exists='append', index=False)
