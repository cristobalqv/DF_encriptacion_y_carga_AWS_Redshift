[https://github.com/cristobalqv/DF_encriptacion_y_carga_AWS_Redshift/blob/main/ciberseguridad.jpeg](https://github.com/cristobalqv/DF_encriptacion_y_carga_AWS_Redshift/blob/main/ciberseguridad.jpeg)
Este repositorio contiene una tabla de base de datos que almacena datos ficticios de una serie de clientes de un banco. La información muestra sus nombres y su información bancaria cifrada.

El código crea un engine de SQLAlchemy para conectarse a una base de datos, generando datos aleatorios utilizando la librería `faker`, guardándolos en un dataframe de pandas y cifrando los datos confidenciales con `hashlib`. Por último estos son cargados en AWS Redshift.

## Estructura del Proyecto

- **Creación del engine de SQLAlchemy**: Configura una conexión a la base de datos de AWS Redshift utilizando SQLAlchemy.

- **Generación y guardado de datos aleatorios**: Utiliza la librería `faker` para generar datos ficticios como nombres, direcciones, y correos electrónicos.

- **Cifrado de datos**: Usa la librería `hashlib` para cifrar datos confidenciales, asegurando la protección de información sensible.

- **Carga a AWS Redshift**: Los datos generados y cifrados se cargan en una tabla de AWS Redshift para análisis posterior.
[![](https://github.com/cristobalqv/DF_encriptacion_y_carga_AWS_Redshift/blob/main/encriptacion%20dbeaver.png)](https://github.com/cristobalqv/DF_encriptacion_y_carga_AWS_Redshift/blob/main/encriptacion%20dbeaver.png)
