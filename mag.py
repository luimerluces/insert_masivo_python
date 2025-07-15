import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import logging

# Configurar el logging básico (alternativa a print())
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'MAG'
}

def get_db_connection():
    """Establece y devuelve una conexión a la base de datos MySQL."""
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        logging.info("Conexión a MySQL establecida con éxito.")
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Error: Usuario o contraseña incorrectos.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error(f"Error: La base de datos '{DB_CONFIG['database']}' no existe.")
        else:
            logging.error(f"Error al conectar a la base de datos: {err}")
        return None

def run_etl_process(file_path='Pago_Movil.csv', delimiter=';'):
    """
    Ejecuta el proceso ETL: lee CSV, filtra, transforma y carga en MySQL.
    """
    df = None # Inicializar df para el bloque finally

    try:
        # 1. Leer el archivo CSV
        # Eliminamos index_col=0 para que DATE_PAY sea una columna normal
        df = pd.read_csv(file_path, delimiter=delimiter, dtype=str)
        logging.info("DataFrame inicial cargado con éxito.")
        logging.info(f"Tipos de datos del DataFrame inicial:\n{df.dtypes}")

        # 2. Transformaciones de datos
        # Actualizar un valor de una columna según un criterio (ya presente en tu código)
        df.loc[df['BANK_ORIGIN'] == '0007', 'BANK_ORIGIN'] = '0175'
        logging.info("Valores de BANK_ORIGIN actualizados donde BANK_ORIGIN era '0007'.")

        # Convertir la columna 'AMOUNT' a numérico
        # Usamos errors='coerce' para convertir valores no válidos a NaN
        df['AMOUNT'] = pd.to_numeric(df['AMOUNT'], errors='coerce')
        # Opcional: eliminar filas donde AMOUNT no pudo ser convertido (es NaN)
        df.dropna(subset=['AMOUNT'], inplace=True)
        logging.info("Columna 'AMOUNT' convertida a tipo numérico y filas con valores inválidos eliminadas.")

        # Filtrar el DataFrame
        df_filtrado_mixed = df[df["STATUS_SWITCHE"] == '00']
        logging.info("DataFrame filtrado por STATUS_SWITCHE == '00'.")

        # Seleccionar columnas deseadas
        columnas_deseadas = ['DATE_PAY','TIME_PAY', 'REFERENC','AMOUNT','BANK_ORIGIN','BANK_DESTINATION']
        # Verificar que las columnas existan antes de seleccionar
        missing_columns = [col for col in columnas_deseadas if col not in df_filtrado_mixed.columns]
        if missing_columns:
            logging.error(f"Error: Las siguientes columnas deseadas no se encontraron en el DataFrame: {missing_columns}")
            return # Salir si faltan columnas
        df_seleccionado = df_filtrado_mixed[columnas_deseadas]
        logging.info("Columnas deseadas seleccionadas.")

        # Convertir a lista de diccionarios para la inserción
        list_of_dicts = df_seleccionado.to_dict(orient='records')
        logging.info(f"Lista de diccionarios resultante generada. Primeros 3 elementos:\n{list_of_dicts[:3]}")

        # 3. Cargar datos en la base de datos MySQL
        conn = None # Inicializar conn para el bloque finally
        try:
            conn = get_db_connection()
            if conn:
                # Usar 'with' para asegurar que el cursor se cierre automáticamente
                with conn.cursor() as cursor:
                    insert_statement = """
                    INSERT INTO MAG (DATE_PAY, TIME_PAY, REFERENC, AMOUNT, BANK_ORIGIN, BANK_DESTINATION)
                    VALUES (%(DATE_PAY)s, %(TIME_PAY)s, %(REFERENC)s, %(AMOUNT)s, %(BANK_ORIGIN)s, %(BANK_DESTINATION)s)
                    """
                    logging.info(f"Sentencia INSERT utilizada:\n{insert_statement.strip()}")

                    # Ejecutar la inserción masiva
                    cursor.executemany(insert_statement, list_of_dicts)
                    
                    # Guardar cambios
                    conn.commit()
                    logging.info(f"Se insertaron {cursor.rowcount} registros con éxito en la tabla MAG.")

        except mysql.connector.Error as err:
            logging.error(f"Error durante la operación de base de datos: {err}")
            if conn:
                conn.rollback() # Revertir cambios en caso de error
                logging.warning("Transacción revertida debido a un error.")
        finally:
            if conn:
                conn.close()
                logging.info("Conexión a la base de datos cerrada.")

    except FileNotFoundError:
        logging.error(f"Error: El archivo '{file_path}' no se encontró. Asegúrate de que esté en la misma ubicación que el script.")
    except pd.errors.EmptyDataError:
        logging.error(f"Error: El archivo '{file_path}' está vacío.")
    except Exception as e:
        logging.critical(f"Ocurrió un error inesperado durante el proceso ETL: {e}", exc_info=True)

# Ejecutar el proceso ETL
if __name__ == "__main__":
    run_etl_process()