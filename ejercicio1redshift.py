#ENCRIPTACION EN UN SOLO SENTIDO (HASHING) CON CARGA A REPOSITORIO REMOTO AWS REDSHIFT

import os
import hashlib

from dotenv import load_dotenv
from modulos.utilsredshift import ConexionYCargaRedshift

load_dotenv() 

credenciales_redshift = {'user' : os.getenv('redshift_user'),
                        'passw' : os.getenv('redshift_pass'),
                        'host' : os.getenv('redshift_host'),
                        'port' : os.getenv('redshift_port'),
                        'database' : os.getenv('redshift_database')}

schema = 'cjquirozv_coderhouse'

conexion_redshift = ConexionYCargaRedshift(credenciales_redshift)
conexion_redshift.crear_engine()
dataframe_no_cifrado = conexion_redshift.crear_datos_devuelve_dataframe()
dataframe_cifrado = conexion_redshift.cifrar_dataframe(dataframe_no_cifrado)
conexion_redshift.cargar_a_redshift(dataframe_cifrado, 'Encriptación_clientes_hash', schema)

conexion_redshift.cerrar_conexion()




