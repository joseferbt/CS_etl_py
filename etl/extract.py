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

def extractCustomer(con: Engine):
    customer = pd.read_sql_table('Customer', con, schema='Sales')
    person = pd.read_sql_table('Person', con, schema='Person')
    territory = pd.read_sql_table('SalesTerritory', con, schema='Sales')

    return [customer, person, territory]

def extractGeography(con: Engine):
    # Leer las tablas excluyendo las columnas problemáticas
    address_query = """
        SELECT City, StateProvinceID, PostalCode
        FROM Person.Address
    """

    address = pd.read_sql_query(address_query, con)
    stateProvince = pd.read_sql_table('StateProvince', con, schema='Person')
    countryRegion = pd.read_sql_table('CountryRegion', con, schema='Person')

    return [address, stateProvince, countryRegion]


def extractSalesTerritory(con: Engine):
    # Leer las tablas excluyendo las columnas problemáticas
    salesTerritory_query = """
        SELECT TerritoryID, Name, CountryRegionCode, "Group"
        FROM Sales.SalesTerritory
    """

    salesTerritory = pd.read_sql_query(salesTerritory_query, con)
    countryRegion = pd.read_sql_table('CountryRegion', con, schema='Person')

    return [salesTerritory, countryRegion]


import pandas as pd
from sqlalchemy.engine import Engine


def extractProduct(con: Engine):
    """
    Extrae datos relacionados con productos de la base de datos AdventureWorks.

    :param con: Conexión del motor SQLAlchemy a la base de datos AdventureWorks
    :return: Diccionario que contiene DataFrames de las tablas relacionadas con productos
    """
    # Extraer datos del esquema Production
    producto_query = """
            SELECT ProductID, Name, ProductNumber, FinishedGoodsFlag, Color, SafetyStockLevel,ReorderPoint,ListPrice, Size, DaysToManufacture, Weight, Style, ProductSubcategoryID, ProductModelID, StandardCost, WeightUnitMeasureCode,DaysToManufacture, ProductLine, Class, SellStartDate, SellEndDate
            FROM Production.Product
        """
    modelo_query = """
                SELECT ProductModelID, Name
                FROM Production.ProductModel
            """
    producto= pd.read_sql_query(producto_query, con)
    subcategoriaProducto = pd.read_sql_table('ProductSubcategory', con, schema='Production')
    modeloProducto = pd.read_sql_query(modelo_query, con)


    return [ producto,subcategoriaProducto,modeloProducto ]



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