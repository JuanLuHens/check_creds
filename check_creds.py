import smtplib
import requests
import json
from datetime import datetime
import urllib3
import modulos.candidatos2 as candidatos2
import modulos.comunidad as comunidad
from modulos.config import SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TO, OPENSEARCH_BASE_URL, OPENSEARCH_USER, OPENSEARCH_PASS, CANDIDATOS, COMUNIDAD
from modulos.db_client import DatabaseClient
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging
from logging.handlers import RotatingFileHandler
import os


# Deshabilitar advertencias de SSL para conexiones internas
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



# Configuración de logging con rotación
LOG_FILE = os.path.join(os.path.dirname(__file__), 'check_creds.log')
logger = logging.getLogger('check_creds')
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def email(lista_credenciales):
    cuerpo_email = generar_cuerpo_email(lista_credenciales)
    smtp_server = SMTP_SERVER or "smtp.office365.com"
    smtp_user = SMTP_USER
    smtp_pass = SMTP_PASS
    smtp_port = SMTP_PORT or 587  
    from_address = SMTP_FROM or SMTP_USER
    to_address = SMTP_TO
    to_address_str = ", ".join(to_address)
    subject = 'Credenciales expuestas darkweb'
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address_str
    msg['Subject'] = subject
    msg.attach(MIMEText(cuerpo_email, 'html'))

    smtp = None
    try:
        smtp = smtplib.SMTP(smtp_server, smtp_port)
        smtp.ehlo()  
        smtp.starttls()  
        smtp.ehlo()  
        smtp.login(smtp_user, smtp_pass)  
        smtp.sendmail(from_address, to_address, msg.as_string())
        logger.info("Correo enviado exitosamente")
    except Exception as e:
        logger.exception(f'Error al enviar el correo: {e}')
    finally:
        if smtp is not None:
            smtp.quit()


def generar_cuerpo_email(lista_credenciales):
    filas_tabla = ""
    for credencial in lista_credenciales:
        usuario = credencial.get('usuario')
        dominio = credencial.get('dominio')
        filas_tabla += f"""
        <tr style="background-color: #ffffff;">
            <td style="padding: 12px; border-bottom: 1px solid #dddddd;">{usuario}</td>
            <td style="padding: 12px; border-bottom: 1px solid #dddddd;">{dominio}</td>
        </tr>"""

# 2. Plantilla HTML completa con el diseño corporativo
    cuerpo = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Alerta de Seguridad</title>
    </head>
    <body style="margin: 0; padding: 0; background-color: #f4f4f4; font-family: Arial, sans-serif;">
        <table border="0" cellpadding="0" cellspacing="0" width="100%">
            <tr>
                <td style="padding: 20px 0;">
                    <table align="center" border="0" cellpadding="0" cellspacing="0" width="600" style="border-collapse: collapse; background-color: #ffffff; border: 1px solid #cccccc; border-radius: 8px; overflow: hidden;">
                        
                        <!-- ENCABEZADO -->
                        <tr>
                            <td align="center" style="padding: 30px 20px; background-color: #003366; color: #ffffff;">
                                <!-- Si tienes un logo, puedes ponerlo aquí -->
                                <!-- <img src="URL_DE_TU_LOGO" alt="Logo de la Empresa" width="150"> -->
                                <h1 style="margin: 0; font-size: 24px;">Alerta de Seguridad</h1>
                            </td>
                        </tr>
                        
                        <!-- CUERPO DEL CONTENIDO -->
                        <tr>
                            <td style="padding: 30px 25px;">
                                <h2 style="color: #FF6600; margin-top: 0;"><center>Credenciales Expuestas Detectadas</center></h2>
                                <p style="color: #333333; line-height: 1.5;">
                                    Nuestro sistema de monitoreo ha detectado que las siguientes credenciales han sido expuestas en la Deep/Dark Web. 
                                    Es crucial revisar y tomar las medidas necesarias de inmediato.
                                </p>
                                <p style="color: #333333; line-height: 1.5;">
                                    Por favor, revise el estado de estas cuentas en el sistema de gestión interna accesible en: 
                                    <a href="https://vacascan.atento.com" style="color: #003366; text-decoration: none; font-weight: bold;">vacascan.atento.com</a> (solo accesible desde VPN).
                                </p>
                                
                                <!-- TABLA DE CREDENCIALES -->
                                <table border="0" cellpadding="0" cellspacing="0" width="100%" style="border-collapse: collapse; margin-top: 20px;">
                                    <thead>
                                        <tr style="background-color: #003366; color: #ffffff;">
                                            <th style="padding: 12px; text-align: left;">Usuario</th>
                                            <th style="padding: 12px; text-align: left;">Dominio</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {filas_tabla}
                                    </tbody>
                                </table>
                                
                                <p style="color: #333333; line-height: 1.5; margin-top: 25px;">
                                    Si tiene alguna pregunta, por favor contacte al equipo de <a href="mailto:red-team@atento.com">RedTeam</a>.
                                </p>
                            </td>
                        </tr>
                        
                        <!-- PIE DE PÁGINA -->
                        <tr>
                            <td style="padding: 20px; background-color: #003366; color: #ffffff; text-align: center; font-size: 12px;">
                                <p style="margin: 0;">© 2025 RedTeam Atento. Todos los derechos reservados.</p>
                                <p style="margin: 5px 0 0 0;">Este es un mensaje automático. Por favor, no responda a este correo.</p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    return cuerpo

def connect_to_opensearch():
    """Conecta a OpenSearch y retorna la sesión"""
    base_url = OPENSEARCH_BASE_URL
    auth = (OPENSEARCH_USER, OPENSEARCH_PASS) if OPENSEARCH_USER and OPENSEARCH_PASS else None
    
    # Verificar conexión
    try:
        response = requests.get(f"{base_url}/_cluster/health", auth=auth, verify=False)
        if response.status_code == 200:
            logger.info("Conexión exitosa a OpenSearch")
            return base_url, auth
        else:
            logger.error(f"Error de conexión a OpenSearch: {response.status_code}")
            return None, None
    except Exception as e:
        logger.exception(f"Error de conexión a OpenSearch: {e}")
        return None, None

def update_revisado(base_url, auth, id, revisado):
    """Actualiza el campo revisado de un registro"""
    url = f"{base_url}/leak_atento_datos_pro/_update/{id}"
    data = {
        "doc": {"revisado": revisado}
    }
    response = requests.post(url, json=data, auth=auth, verify=False)
    if response.status_code == 200:
        logger.info(f"Registro {id} actualizado correctamente")
    else:
        logger.error(f"Error al actualizar el registro {id}: {response.status_code}")


def get_unreviewed_records(base_url, auth, index="leak_atento_datos_pro", size=1000):
    """Obtiene TODOS los registros no revisados del índice usando paginación con search_after.
    El parámetro size es el tamaño de página.
    """
    all_hits = []
    search_after = None
    total_hits_reported = None

    while True:
        query = {
            "query": {
                "term": {
                    "revisado": "NO"
                }
            },
            "size": size,
            "sort": [
                {"_id": {"order": "asc"}}
            ]
        }
        if search_after is not None:
            query["search_after"] = [search_after]

        try:
            url = f"{base_url}/{index}/_search"
            response = requests.post(url, json=query, auth=auth, verify=False)
            
            if response.status_code != 200:
                logger.error(f"Error en la consulta: {response.status_code} - {response.text}")
                break

            data = response.json()
            if total_hits_reported is None:
                total_hits_reported = data.get('hits', {}).get('total', {}).get('value', 0)
                logger.info(f"Total de registros no revisados (reportado): {total_hits_reported}")

            hits = data.get('hits', {}).get('hits', [])
            if not hits:
                break

            all_hits.extend(hits)
            search_after = hits[-1].get('sort', [None])[0]

            logger.info(f"Recuperados {len(hits)} (acumulado {len(all_hits)}/{total_hits_reported})")

            # Seguridad: si por alguna razón no viene el campo sort, evitamos bucle infinito
            if search_after is None:
                break
        except Exception as e:
            logger.exception(f"Error al obtener registros: {e}")
            break

    return all_hits

def get_index_stats(base_url, auth, index="leak_atento_datos_pro"):
    """Obtiene estadísticas del índice"""
    try:
        url = f"{base_url}/{index}/_stats"
        response = requests.get(url, auth=auth, verify=False)
        
        if response.status_code == 200:
            data = response.json()
            stats = data['indices'][index]['total']
            print(f"\n Estadísticas del índice '{index}':")
            print(f"  Total de documentos: {stats['docs']['count']}")
            print(f"  Tamaño del índice: {stats['store']['size_in_bytes'] / (1024*1024):.2f} MB")
            return stats
        else:
            print(f"❌ Error al obtener estadísticas: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error al obtener estadísticas: {e}")
        return None

def check_creds(item, database_client):
    lista = []
    logger.info('Usuario correcto')
    credenciales = {
        'id': item['_id'],
        'usuario': item['_source']['usuario'],
        'dominio': item['_source']['dominio'],
        'password': item['_source']['password']
    }
    lista.append(credenciales)
    df = pd.DataFrame(lista)
    database_client.insert_data(df, 'creds_atento')
    return credenciales

def main():
    """Función principal"""
    logger.info("Script de consulta de OpenSearch - Registros no revisados")
    logger.info("=" * 60)
    
    # Conectar a OpenSearch
    base_url, auth = connect_to_opensearch()
    if not base_url:
        return
    
    # Obtener estadísticas del índice
    get_index_stats(base_url, auth)
    
    # Obtener registros no revisados (todos, paginando)
    logger.info(f"Buscando TODOS los registros no revisados en 'leak_atento_datos_pro'...")
    records = get_unreviewed_records(base_url, auth, size=1000)
    database_client = DatabaseClient()
    lista_credenciales = []
    
    #dominios
    candidatos = CANDIDATOS
    comunidad = COMUNIDAD
    if records is not None:
        logger.info(f"Consulta completada. Se recuperaron {len(records)} registros")
        
        for item in records:
                if item['_source']['dominio'] == candidatos:
                    logger.info(f"{item['_source']['usuario']} {item['_id']}")
                    resultado = candidatos2.login_success(item['_source']['usuario'],item['_source']['password'])
                    if resultado:
                        lista_credenciales.append(check_creds(item, database_client))
                    update_revisado(base_url, auth, item['_id'], 'Si')
                elif item['_source']['dominio'] == comunidad:
                    logger.info(f"{item['_source']['usuario']} {item['_id']}")
                    resultado = comunidad.login_success(item['_source']['usuario'],item['_source']['password'])
                    if resultado:
                        lista_credenciales.append(check_creds(item, database_client))
                    update_revisado(base_url, auth, item['_id'], 'Si')
        if len(lista_credenciales) > 0:
            email(lista_credenciales)                 
    else:
        logger.error("No se pudieron obtener los registros")
    

if __name__ == "__main__":
    main()
