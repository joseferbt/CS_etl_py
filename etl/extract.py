import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData


def extract(con):
    """
    :param con: the connection to the database
    :return:
    """


def extractCurrency(con: Engine):
    """
    Extract data from database where the conexion established
    :param con:
    :return:
    """
    dimCurrency = pd.read_sql_table('Currency', con, schema='Sales')
    #print(dimCurrency.head())
    #dimCurrency.head()
    #dimCurrency.info()
    return dimCurrency

#
# def extract_medico(con: Engine):
#     dim_medico = pd.read_sql_table('medico', con)
#     return dim_medico
#
# def extract_persona(con: Engine):
#     beneficiarios = pd.read_sql_table("beneficiario", con)
#     cotizantes = pd.read_sql_table("cotizante", con)
#     cot_ben = pd.read_sql_table("cotizante_beneficiario", con)
#     return [beneficiarios, cotizantes, cot_ben]
#
# def extract_trans_servicio(con: Engine):
#     df_citas = pd.read_sql_table('citas_generales', con)
#     df_urgencias = pd.read_sql_table('urgencias', con)
#     df_hosp = pd.read_sql_table('hospitalizaciones', con)
#     return [df_citas, df_urgencias, df_hosp]
#
# def extract_hehco_atencion(con: Engine):
#     df_trans = pd.read_sql_table('trans_servicio', con)
#     dim_persona = pd.read_sql_table('dim_persona', con)
#     dim_medico = pd.read_sql_table('dim_medico', con)
#     dim_servicio = pd.read_sql_table('dim_servicio', con)
#     dim_ips = pd.read_sql_table('dim_ips', con)
#     dim_fecha = pd.read_sql_table('dim_fecha', con)
#     return [df_trans,dim_persona,dim_medico,dim_servicio,dim_ips,dim_fecha]