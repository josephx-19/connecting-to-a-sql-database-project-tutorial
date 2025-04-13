import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener los parámetros de conexión con valores por defecto
db_user = os.getenv("DB_USER", 'jose').lower()
db_pass = os.getenv("DB_PASS", 'jose').lower()
db_host = os.getenv("DB_HOST", 'localhost')
db_name = os.getenv("DB_NAME", 'proyectosql')

# Construir la cadena de conexión para PostgreSQL
connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"

# Crear el engine de SQLAlchemy
engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")

def execute_sql_file(filepath):
    """
    Lee y ejecuta el contenido de un archivo SQL.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            sql_commands = file.read()
        with engine.begin() as connection:
            connection.execute(text(sql_commands))
        print(f"Archivo {filepath} ejecutado exitosamente.")
    except Exception as e:
        print(f"Error al ejecutar {filepath}: {e}")

def main():
    # Construir rutas basadas en la ubicación actual de app.py
    BASE_DIR = os.path.dirname(__file__)
    create_sql_path = os.path.join(BASE_DIR, "sql", "create.sql")
    insert_sql_path = os.path.join(BASE_DIR, "sql", "insert.sql")
    
    # 1) Crear las tablas ejecutando el script de creación
    execute_sql_file(create_sql_path)

    # 2) Insertar datos ejecutando el script de inserción
    execute_sql_file(insert_sql_path)

    # 3) Leer la tabla 'books' usando pandas y mostrarla
    try:
        df_books = pd.read_sql("SELECT * FROM books", engine)
        print("Tabla 'books':")
        print(df_books)
    except Exception as e:
        print(f"Error al leer la tabla 'books': {e}")

if __name__ == "__main__":
    main()


