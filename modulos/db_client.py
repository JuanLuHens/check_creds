#interactua con la DB
from sqlalchemy import create_engine, text, types
import logging
import pandas as pd
from .config import USER_DB, PASSWORD_DB, SERVER_DB, DATABASE_DB

# Usar el mismo logger que configura check_creds.py
logger = logging.getLogger('check_creds')

class DatabaseClient:
    def __init__(self):
        self.db_url = f"mssql+pyodbc://{USER_DB}:{PASSWORD_DB}@{SERVER_DB}/{DATABASE_DB}?driver=ODBC+Driver+17+for+SQL+Server"
        self.engine = create_engine(self.db_url)

    def insert_data(self, df, table_name):
        try:
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            #logger.info(f"Datos insertados correctamente en la tabla {table_name}")
        except Exception as e:
            logger.error(f"Error al insertar datos en la tabla {table_name}: {e}")
            
    def select_data(self, query):
        try:
            df = pd.read_sql(query, self.engine)  # Ejecuta la consulta SQL y guarda el resultado en un DataFrame
            #logger.info(f"Consulta {query} ejecutada correctamente.")
            return df
        except Exception as e:
            logger.error(f"Error al ejecutar la consulta {query}: {e}")
            return None

    def update_single_row(self, df, table_name):
        """ Actualiza una sola fila sin crear una tabla temporal """
        try:
            # Aquí construimos la consulta de actualización directa
            row = df.iloc[0]  # La primera (y única) fila

            set_clause = ", ".join([f"{col} = '{row[col]}'" for col in df.columns if col != 'id'])
            update_query = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE id = '{row['id']}'
            """
            with self.engine.begin() as conn:
                conn.execute(text(update_query))  # Ejecutamos la consulta de actualización
            #logger.info(f"Fila {row['id']} actualizada correctamente.")
        except Exception as e:
            logger.error(f"Error al actualizar la fila {df.iloc[0]['id']}: {e}")
            raise
    

    

