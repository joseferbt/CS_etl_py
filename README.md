# CS_etl_py
Python etl for a health care database 
## Requirements installation 
 **if not exists environment create one**
```
#unix systems
python3 -m venv my_env
source my_env/bin/activate  
#win
python3 -m venv my_env
#cmd.exe
C:\> <venv>\Scripts\activate.bat
#PowerShell
PS C:\> <venv>\Scripts\Activate.ps1
```
your terminal should look like
```
(my_env) $
```
here you can install the packages by doing 
```
pip install -r requirements.txt
```
structure of config.yml 
```
nombre_conexion:
  drivername: postgresql  
  user: postgres # su username
  password : valor_privado
  port: 5432 # pordefecto 
  host: localhost # la direccion a la base de datos
  dbname: colombia_saludable #nombre de la base de datos
```