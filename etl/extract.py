import pandas as pd
from sqlalchemy.engine import Engine


def extract(tables : list,con: Engine)-> pd.DataFrame:
    """
    :param con: the connection to the database
    :param tables: the tables to extract
    :return: a list of tables in df format
    """
    a = []
    for i in tables:
        aux = pd.read_sql_table(i, con)
        a.append(aux)
    return a

def extract_ips(con: Engine):
    """
    Extract data from database where the conexion established
    :param con:
    :return:
    """
    dim_ips = pd.read_sql_table('ips', con)
    return dim_ips
def extract_medico(con: Engine):
    dim_medico = pd.read_sql_table('medico', con)
    return dim_medico

def extract_persona(con: Engine):
    beneficiarios = pd.read_sql_table("beneficiario", con)
    cotizantes = pd.read_sql_table("cotizante", con)
    cot_ben = pd.read_sql_table("cotizante_beneficiario", con)
    return [beneficiarios, cotizantes, cot_ben]

def extract_trans_servicio(con: Engine):
    df_citas = pd.read_sql_table('citas_generales', con)
    df_urgencias = pd.read_sql_table('urgencias', con)
    df_hosp = pd.read_sql_table('hospitalizaciones', con)
    return [df_citas, df_urgencias, df_hosp]

def extract_hehco_atencion(con: Engine):
    df_trans = pd.read_sql_table('trans_servicio', con)
    dim_persona = pd.read_sql_table('dim_persona', con)
    dim_medico = pd.read_sql_table('dim_medico', con)
    dim_servicio = pd.read_sql_table('dim_servicio', con)
    dim_ips = pd.read_sql_table('dim_ips', con)
    dim_fecha = pd.read_sql_table('dim_fecha', con)
    return [df_trans,dim_persona,dim_medico,dim_servicio,dim_ips,dim_fecha]

def extract_medicamentos(con: Engine):
    df_medicamentos = pd.read_excel('sources/medicamentos.xls')
    return df_medicamentos
def extract_pagos_retiros(con: Engine):
    df_pagos = pd.read_sql_table('pagos', con)
    df_retiros = pd.read_sql_table('retiros', con)
    return df_pagos, df_retiros
def extract_drogeria(con: Engine):
    df_drogeria = pd.read_sql_table('drogeria', con)
def extract_empresa(con: Engine):
    df_empresa = pd.read_sql_table('empresa', con)
def extract_demographics(con: Engine):
    df_benco= pd.read_sql_table('cotizante_beneficiario', con)
    df_cotizantes = pd.read_sql_query(
        '''select cedula as numero_identificacion, tipo_cotizante, estado_civil, sexo, fecha_nacimiento,
            nivel_escolaridad, estracto, proviene_otra_eps,salario_base,tipo_discapacidad,id_ips from cotizante''', con)
    df_beneficiarios = pd.read_sql_query(
        '''select id_beneficiario as numero_identificacion, fecha_nacimiento, sexo, estado_civil,
         tipo_discapacidad from beneficiario ''', con)
    df_ips = pd.read_sql_query('select id_ips,municipio,departamento from ips', con )
    return [df_benco,df_cotizantes,df_beneficiarios,df_ips]