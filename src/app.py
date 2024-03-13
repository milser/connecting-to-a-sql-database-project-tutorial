import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function
#con_str = "postgresql://gitpod:postgres:@localhost/milser_pg"
global engine # This allows us to use a global variable called engine
# A "connection string" is basically a string containing all database credentials together.
connection_string = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')}"

#engine = create_engine(connection_string)
#Session = sessionmaker(bind=engine,autocommit=False) 

try: 
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    print("connected")

    with open('./src/sql/drop.sql', 'r') as file:
        drop_sql = file.read().rstrip()
    
    cursor.execute(drop_sql)
    # 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
    with open('./src/sql/create.sql', 'r') as file:
        create_sql = file.read().rstrip()
    
    cursor.execute(create_sql)

    # 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function
    with open('./src/sql/insert.sql', 'r') as file:
        insert_sql = file.read().rstrip()
        
    cursor.execute(insert_sql)
    conn.commit()
    print("éxito")

except Exception as e:
    print("error")
    print(e)

finally:
    # Cierra la sesión
    conn.close()


# 4) Use pandas to print one of the tables as dataframes using read_sql function
# También se pueden almacenar los resultados en un DataFrame usando Pandas
conn = psycopg2.connect(connection_string)
cursor_df = pd.read_sql(sql="SELECT * FROM books",con= conn)
print(cursor_df)


