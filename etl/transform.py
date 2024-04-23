from datetime import timedelta, date

import holidays
import numpy as np
import pandas as pd
from pandas import DataFrame


def transform_ips(dim_ips: DataFrame) -> DataFrame:
    dim_ips.replace({'': '0'}, inplace=True)
    dim_ips["saved"] = date.today()
    return dim_ips


def transform_medico(dim_medico: DataFrame) -> DataFrame:
    dim_medico.replace({np.nan: 'no aplica', ' ': 'no aplica','':'no_aplica'}, inplace=True)
    dim_medico["saved"] = date.today()
    return dim_medico


def transform_persona(args) -> DataFrame:
    beneficiarios, cotizantes, cot_ben = args
    cotizantes.rename(columns={'cedula': 'numero_identificacion'}, inplace=True)
    cotizantes.drop(
        columns=['direccion', 'tipo_cotizante', 'nivel_escolaridad', 'estracto', 'proviene_otra_eps', 'salario_base',
                 'fecha_afiliacion', 'id_ips'], inplace=True)
    cotizantes['tipo_documento'] = "cedula"
    cotizantes['tipo_usuario'] = "cotizante"
    cotizantes['grupo_familiar'] = cotizantes['numero_identificacion']
    beneficiarios.drop(columns=['parentesco'], inplace=True)
    beneficiarios.rename(columns={'tipo_identificacion': 'tipo_documento', 'id_beneficiario': 'numero_identificacion'},
                         inplace=True)
    beneficiarios['tipo_usuario'] = "beneficiario"
    beneficiario = beneficiarios.merge(cot_ben, left_on='numero_identificacion', right_on='beneficiario', how='left')
    beneficiario.rename(columns={'cotizante': 'grupo_familiar'}, inplace=True)
    beneficiario.drop(columns=['beneficiario'], inplace=True)
    dim_persona = pd.concat([beneficiario, cotizantes])
    dim_persona["saved"] = date.today()
    dim_persona.reset_index(drop=True, inplace=True)

    return dim_persona


def transform_servicio() -> DataFrame:
    dim_servicio = pd.DataFrame({
        'name': ['citas', 'hospitalizacion', 'urgencias'],
        'descripcion': ['servicio de citas medicas', 'servicio de hospitalizacion', 'servicio de urgencias']
    })
    return dim_servicio


def transform_fecha() -> DataFrame:
    dim_fecha = pd.DataFrame({"date": pd.date_range(start='1/1/2005', end='1/1/2009', freq='D')})
    dim_fecha["year"] = dim_fecha["date"].dt.year
    dim_fecha["month"] = dim_fecha["date"].dt.month
    dim_fecha["day"] = dim_fecha["date"].dt.day
    dim_fecha["weekday"] = dim_fecha["date"].dt.weekday
    dim_fecha["quarter"] = dim_fecha["date"].dt.quarter
    dim_fecha["day_of_year"] = dim_fecha["date"].dt.day_of_year
    dim_fecha["day_of_month"] = dim_fecha["date"].dt.days_in_month
    dim_fecha["month_str"] = dim_fecha["date"].dt.month_name()  # run locale -a en unix
    dim_fecha["day_str"] = dim_fecha["date"].dt.day_name()  # locale = 'es_CO.UTF8'
    dim_fecha["date_str"] = dim_fecha["date"].dt.strftime("%d/%m/%Y")
    co_holidays = holidays.CO(language="es")
    dim_fecha["is_Holiday"] = dim_fecha["date"].apply(lambda x: x in co_holidays)
    dim_fecha["holiday"] = dim_fecha["date"].apply(lambda x: co_holidays.get(x))
    dim_fecha["weekend"] = dim_fecha["weekday"].apply(lambda x: x > 4)
    dim_fecha["saved"] = date.today()
    return dim_fecha

def transform_trans_servicio(args) -> DataFrame:
    df_citas, df_urgencias, df_hosp = args
    df_hosp.rename(columns={'codigo_hospitalizacion': 'codigo_servicio'}, inplace=True)
    df_urgencias.rename(columns={'codigo_urgencia': 'codigo_servicio'}, inplace=True)
    df_citas.rename(columns={'codigo_cita': 'codigo_servicio'}, inplace=True)

    df_citas['tipo_servicio'] = 'citas'
    df_urgencias['tipo_servicio'] = 'urgencias'
    df_hosp['tipo_servicio'] = 'hospitalizacion'

    columns = ['codigo_servicio', 'id_usuario', 'id_medico', 'fecha_solicitud', 'fecha_atencion', 'hora_atencion',
               'hora_solicitud', 'tipo_servicio']
    trans_servicio = pd.concat([df_hosp, df_urgencias, df_citas], axis=0)
    trans_servicio.head()
    del_columns = set(trans_servicio.columns) - set(columns)
    trans_servicio.drop(columns=del_columns, inplace=True)
    trans_servicio['fecha_atencion'] = pd.to_datetime(trans_servicio['fecha_atencion'])
    trans_servicio['fecha_solicitud'] = pd.to_datetime(trans_servicio['fecha_solicitud'])
    trans_servicio['hora_atencion'] = trans_servicio['hora_atencion'].apply(
        lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
    trans_servicio['hora_solicitud'] = trans_servicio['hora_solicitud'].apply(
        lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
    trans_servicio['fecha_hora_atencion'] = trans_servicio['fecha_atencion'] + trans_servicio['hora_atencion']
    trans_servicio['fecha_hora_solicitud'] = trans_servicio['fecha_solicitud'] + trans_servicio['hora_solicitud']
    trans_servicio["saved"] = date.today()
    trans_servicio.reset_index(drop=True, inplace=True)
    return trans_servicio
def transform_hecho_atencion(args) -> DataFrame:
    df_trans, dim_persona, dim_medico, dim_servicio, dim_ips, dim_fecha = args
    hecho_atencion = pd.merge(df_trans, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_atencion', right_on='date')
    hecho_atencion.drop(columns=['date'], inplace=True)
    hecho_atencion.rename(
        columns={'key_dim_fecha': 'key_fecha_atencion', 'id_medico': 'cedula', 'id_usuario': 'numero_identificacion'},
        inplace=True)
    hecho_atencion = pd.merge(hecho_atencion, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_solicitud',
                              right_on='date')
    hecho_atencion.drop(columns=['date'], inplace=True)
    hecho_atencion.rename(columns={'key_dim_fecha': 'key_fecha_solicitud'}, inplace=True)
    hecho_atencion = hecho_atencion.merge(dim_persona[['key_dim_persona', 'numero_identificacion']])
    hecho_atencion.drop(columns=['numero_identificacion'], inplace=True)
    hecho_atencion = hecho_atencion.merge(dim_medico[['key_dim_medico', 'cedula', 'id_ips']])
    hecho_atencion.drop(columns=['cedula'], inplace=True)
    hecho_atencion = hecho_atencion.merge(dim_ips[['key_dim_ips', 'id_ips']])
    hecho_atencion.drop(columns=['id_ips'], inplace=True)
    hecho_atencion = hecho_atencion.merge(dim_servicio[['name', 'key_dim_servicio']], left_on='tipo_servicio',
                                          right_on='name')
    hecho_atencion.drop(columns=['name', 'tipo_servicio'], inplace=True)
    hecho_atencion['tiempo_espera'] = hecho_atencion['fecha_hora_atencion'] - hecho_atencion['fecha_hora_solicitud']
    hecho_atencion['tiempo_espera_dias'] = hecho_atencion['tiempo_espera'].dt.days
    hecho_atencion['tiempo_espera_minutos'] = hecho_atencion['tiempo_espera'].dt.seconds // 60
    hecho_atencion['tiempo_espera_horas'] = hecho_atencion['tiempo_espera'].dt.seconds // (60 * 60)
    hecho_atencion['tiempo_espera_segundos'] = hecho_atencion['tiempo_espera'].dt.seconds
    hecho_atencion["saved"] = date.today()

    hecho_atencion.drop(
        columns=['tiempo_espera', 'fecha_atencion', 'fecha_solicitud', 'hora_solicitud', 'hora_atencion',
                 'fecha_hora_solicitud', 'fecha_hora_atencion', 'codigo_servicio'], inplace=True)

    return hecho_atencion