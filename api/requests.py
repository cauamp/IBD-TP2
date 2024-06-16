import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE = os.getenv("DATABASE_URL")


def connect_db():
	try:
		conn = psycopg2.connect(DATABASE)
		print("Conexão bem sucedida ao banco de dados PostgreSQL!")
		return conn
	except psycopg2.Error as e:
		print("Erro ao conectar ao banco de dados PostgreSQL:", e)
		return None


conn = connect_db()
cursor = conn.cursor()
conn.close()