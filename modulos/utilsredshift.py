import pandas as pd
import psycopg2
import dotenv
import random
import hashlib

from sqlalchemy import create_engine
from faker import Faker


class ConexionYCargaRedshift:
    def __init__(self, credenciales:dict):
        self.credenciales = credenciales
        self.engine = None


    def crear_engine(self):
        if self.engine == None:
            # url de conexion
            connection_url = f"postgresql+psycopg2://{self.credenciales['user']}:{self.credenciales['passw']}@{self.credenciales['host']}:{self.credenciales['port']}/{self.credenciales['database']}"
            try:
                self.engine = create_engine(connection_url)
                print('Engine creado con éxito')

                #Verificamos que conexión ande bien
                try:
                    with self.engine.connect() as connection:
                        print('Conexión establecida con éxito')
                except Exception as e:
                    print(f'No se pudo establecer conexion: {e}')
            except Exception as e:
                print(f'Hubo una excepción al crear el engine: {e}')
        else:
            print('conexión actualmente activa')


    def crear_datos_devuelve_dataframe(self, numero_registros=30):
        #Creamos un dataframe con datos aleatorios:
        fake = Faker()
        data = []
        for numero in range(1, numero_registros+1):
            try:    
                customer_id = fake.random_int(min=1, max=1000)
                first_name = fake.first_name()
                last_name= fake.last_name()
                phone_number = fake.phone_number()     #PII
                email = fake.email()            #PII
                account_balance = round(random.uniform(100, 10000), 2)      #PII
                credit_card_number = fake.credit_card_number()             #PII
                card_expiration_date = fake.date_between_dates(date_start=datetime.today(), date_end=datetime.today() + timedelta(days=365*5))
                card_cvv = str(fake.random_int(min=100, max=999)).zfill(3)

                data.append({
                            "customer_id": customer_id,
                            "first_name": first_name,
                            "last_name": last_name,
                            "phone_number": phone_number,
                            "email": email,
                            "account_balance": account_balance,
                            "credit_card_number": credit_card_number,
                            "card_expiration_date": card_expiration_date,
                            "card_cvv": card_cvv})
            except Exception as e:
                print(f'Hubo un error al crear los datos: {e}')
        print('Creando dataframe...')
        dataframe = pd.DataFrame(data) 
        print('Dataframe creado con éxito')
        return dataframe
    

    def cifrar_dataframe(self, dataframe:pd.DataFrame):
        df = dataframe
        #libreria hash aplica función solo sobre strings
        try:            
            df['phone_number'] = df['phone_number'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
            df['email'] = df['email'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
            df['account_balance'] = df['account_balance'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
            df['credit_card_number'] = df['credit_card_number'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
            df['card_expiration_date'] = df['card_expiration_date'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())
            df['card_cvv'] = df['card_cvv'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest())

        except Exception as e:
            print(f'Hubo un error al cifrar las columnas clave: {e}')
        print('Dataframe cifrado con éxito')
        return df 


    def cargar_a_redshift(self, dataframe:pd.DataFrame, nombre_tabla, schema):
        if self.engine:
            try:
                dataframe.to_sql(nombre_tabla, self.engine, schema, if_exists='append', index=False, method='multi')
                print(f'Dataframe cargado con éxito a AWS Redshift. Longitud del dataframe: {len(dataframe)} filas')
            except Exception as e:
                print(f'Error al cargar datos: {e}')
        else:
            print('Problemas para cargar el dataframe')


    def cerrar_conexion(self):
        if self.engine:
            self.engine.dispose()
            print('Conexion cerrada')
        else:
            print('no se puede cerrar la conexion')





    