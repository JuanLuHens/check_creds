#este se encarga de cargar las variables desde el archivo .env
import os
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

SERVER_DB = os.getenv("SERVER_DB")
USER_DB = os.getenv("USER_DB")
PASSWORD_DB = os.getenv("PASSWORD_DB")
DATABASE_DB = os.getenv("DATABASE_DB")

# Configuración SMTP
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT")) if os.getenv("SMTP_PORT") else None
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_FROM = os.getenv("SMTP_FROM")

# Lista de destinatarios separada por comas en .env, por ejemplo: "a@b.com,c@d.com"
_smtp_to_raw = os.getenv("SMTP_TO", "")
SMTP_TO = [email.strip() for email in _smtp_to_raw.split(",") if email.strip()]

# OpenSearch
OPENSEARCH_BASE_URL = os.getenv("OPENSEARCH_BASE_URL")
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS")


#Dominios
CANDIDATOS = os.getenv("CANDIDATOS")
COMUNIDAD = os.getenv("COMUNIDAD")
CGI = os.getenv("CGI")
CANDIDATOSINTERFILE = os.getenv("CANDIDATOSINTERFILE")
PESSOAS_ONLINE = os.getenv("PESSOAS_ONLINE")