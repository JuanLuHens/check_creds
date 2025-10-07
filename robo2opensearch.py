import requests
import re
from urllib.parse import urlparse
import os, glob
import sys
from modulos.config import OPENSEARCH_BASE_URL, OPENSEARCH_USER, OPENSEARCH_PASS
import tldextract
import shutil
from datetime import datetime
import json
import urllib3
# Deshabilitar warnings de HTTPS no verificado (solo si usas verify=False conscientemente)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def connect_to_opensearch(index):
    return f"{OPENSEARCH_BASE_URL}/{index}/_bulk"  

def insert_db(line, revisar, file_name, port, tabla, bulk_data):
    try:
        if port:
            st_url, st_port, st_usuario, st_password = re.split(r'[:|]', line)
            st_url = f"{st_url}:{st_port}"
        else:
            if tabla == 'leak_only_creds_pro':
                st_usuario, st_password = re.split(r'[:|]', line)
                st_url = 'localhost.homelan'
            elif tabla == 'leak_atento_error_pro':
                st_url = line
                st_usuario = 'UNKNOWN'
                st_password = 'UNKNOWN'
            else:
                st_url, st_usuario, st_password = re.split(r'[:|]', line)
        if tabla == 'leak_atento_error_pro':
            st_dominio = 'UNKNOWN'
        else:
            st_dominio = extraer_dominio_base(st_url)
        
        data = {
            "url": st_url,
            "usuario": st_usuario,
            "password": st_password,
            "dominio": st_dominio,
            "file_name": file_name,
            "revisado": "NO"  
        }

        bulk_data.append({'index': {}})
        bulk_data.append(data)
        
        return bulk_data

    except Exception as e:
        print(f"Error general: {e}")
        revisar.write(line + '\n')
        return bulk_data


def send_to_opensearch(index, bulk_data):
    try:
        opensearch_url = connect_to_opensearch(index)
        payload = "\n".join([json.dumps(item) for item in bulk_data]) + "\n"
        
        auth = (OPENSEARCH_USER, OPENSEARCH_PASS)
        response = requests.post(opensearch_url, headers={'Content-Type': 'application/x-ndjson'}, data=payload, auth=auth, verify=False)

        if response.status_code == 200:
            print(f"Datos enviados correctamente al índice {index}.")
        else:
            print(f"Error al insertar en OpenSearch: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error al enviar los datos a OpenSearch: {e}")

def extraer_dominio_base(url):
    ext = tldextract.extract(url)
    if ext.fqdn:
        return ext.fqdn
    else:
        return ext.domain


def process_file_and_insert(file_path, file_log):
    regex = r'^[^:|]+[:|][^:|]+[:|][^:|]+$'
    regex_port = r'^[^:|]+[:|][^:|]+[:|][^:|]+[:|][^:|]+$'
    patron_dominio_aplicacion = re.compile(r'^(https?:?//)?(?:www\.)?[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?::\d{1,5})?')
    patron_ip = re.compile(r'^(https?:?//)?([0-9]{1,3}\.){3}[0-9]{1,3}(?::\d{1,5})?')
    regex_cred = r'^[^:|]+[:|][^:|]+$'
    file_name = os.path.basename(file_path)
    revisar_file = f'/opt/TelegramDownloader/revisar/{file_name}_revisar.txt'
    contador = 0
    resultado = 0
    lineas = 0
    nuevos_registros = False
    commit_interval = 5000  
    bulk_buffers = {}  
    registros_insertados = 0
    # Comprobar si en las primeras líneas del archivo aparece "URL:" o "SOFTWARE:", si no, continuar desde la línea 96
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f_check:
        primeras_lineas = [next(f_check, '') for _ in range(20)]
        contiene_url_o_software = any(
            linea.strip().upper().startswith("URL:") or linea.strip().upper().startswith("SOFT:") 
            for linea in primeras_lineas
        )
    if contiene_url_o_software:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f, open(revisar_file, 'a') as revisar:
            linea_ordenada = []
            bloque = {"URL": None, "USER": None, "PASS": None}
            for linea in f:
                linea = linea.strip()
                if linea.startswith("SOFT:"):
                    # Nuevo bloque, reiniciar
                    bloque = {"URL": None, "USER": None, "PASS": None}
                elif linea.startswith("URL:"):
                    bloque["URL"] = linea[4:].strip()
                elif linea.startswith("USER:"):
                    bloque["USER"] = linea[5:].strip()
                elif linea.startswith("PASS:"):
                    bloque["PASS"] = linea[5:].strip()
                    # Cuando tenemos los tres campos, los unimos y guardamos
                    if bloque["URL"] and bloque["USER"] and bloque["PASS"]:
                        linea_ordenada.append(f"{bloque['URL']}:{bloque['USER']}:{bloque['PASS']}")
                        bloque = {"URL": None, "USER": None, "PASS": None}
    # Al finalizar el procesamiento, exportar linea_ordenada a un archivo de texto, una línea por registro
    if linea_ordenada:
        archivo_exportacion = f'/opt/TelegramDownloader/finalizado/{file_name}_ordenado.txt'
        file_path = archivo_exportacion
        with open(archivo_exportacion, 'w', encoding='utf-8') as f_export:
            for registro in linea_ordenada:
                f_export.write(registro + '\n')
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f, open(revisar_file, 'a') as revisar:
        for line in f:
            lineas += 1
            try:
                if line.startswith('moz-extension'):
                    line = ''
                if re.search(r'atento', line, re.IGNORECASE):
                    tabla = 'leak_atento_datos_pro'
                    nuevos_registros = True
                else:
                    tabla = 'leak_clientes_datos_pro'
                contador += 1
                line = line.strip().replace(' ', ':')
                if line.endswith(':') or line.endswith('|'):
                    line = line[:-1]
                line = re.sub(r'Application::.*', '', line)
                line = re.sub(r'\[UNKNOWN:or:V70\]', 'UNKNOWN', line)
                line = re.sub(r'\[NOT_SAVED\]', 'UNKNOWN', line)
                line = re.sub(r'urn:hbo:page:home', '', line)
                line = re.sub(r':{2,}', ':', line)
                dominio_match = patron_dominio_aplicacion.match(line)
                ip_match = patron_ip.match(line)
                es_aplicacion = False
                if re.search(r'^android:?//.*', line, re.IGNORECASE):
                    es_aplicacion = True
                line = re.sub(r'^https?:?//', '', line)
                line = re.sub(r'^android:?//.*?@', '', line)
                es_dominio = False
                if dominio_match or ip_match:
                    es_dominio = True
                if line.startswith('localhost'):
                    es_dominio = True
                if re.match(regex, line):
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif re.match(regex_port, line):
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, True, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif tabla == 'leak_atento_datos_pro':
                    tabla = 'leak_atento_error_pro'
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif len(re.split(r'[:|]', line)) >= 5:
                    parts = re.split(r'[:|]', line)
                    userline = '_'.join(parts[1:(len(parts)-1)])
                    line = f'{parts[0]}:{userline}:{parts[len(parts)-1]}'
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif re.match(regex_cred, line) and es_dominio == False:
                    if es_aplicacion == False:
                        tabla = 'leak_only_creds_pro'
                    else:
                        line = line.replace('_', ':', 1)
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif re.match(regex, line) and es_aplicacion:
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif len(re.split(r'[:|]', line)) == 4 and 'http' in re.split(r'[:|]', line)[2]:
                    line = re.sub(r'https?://', '', line)
                    parts = re.split(r'[:|]', line)
                    line = f"{parts[2]}:{parts[0]}:{parts[1]}"
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif len(re.split(r'[:|]', line)) >= 5:
                    parts = re.split(r'[:|]', line)
                    userline = '_'.join(parts[1:(len(parts)-1)])
                    line = f'{parts[0]}:{userline}:{parts[len(parts)-1]}'
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                elif tabla == 'leak_atento_datos_pro':
                    tabla = 'leak_atento_error_pro'
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                    registros_insertados += 1
                else:
                    tabla = 'leak_atento_error_pro'
                    bulk_buffers.setdefault(tabla, [])
                    bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
                
                for indice_tabla, buffer in list(bulk_buffers.items()):
                    if (len(buffer) // 2) >= commit_interval:
                        send_to_opensearch(indice_tabla, buffer)
                        bulk_buffers[indice_tabla].clear()

            except Exception as e:
                print(f"Error al procesar la línea: {e}")
                tabla = 'leak_atento_error_pro'
                bulk_buffers.setdefault(tabla, [])
                bulk_buffers[tabla] = insert_db(line, revisar, file_name, False, tabla, bulk_buffers[tabla])
    
    # Hacer el último commit para los datos restantes por índice
    for indice_tabla, buffer in bulk_buffers.items():
        if buffer:
            send_to_opensearch(indice_tabla, buffer)
    
    fecha_hora_actual = datetime.now()
    with open(file_log, 'a') as log:
        log.write(f'\n El fichero tenía {lineas} líneas y ha insertado {registros_insertados} registros.')
    if nuevos_registros:
        #email(file_name)
        print()

def obtener_archivos_txt_ordenados(carpeta):
    archivos_txt = [f for f in os.listdir(carpeta) if f.endswith('.txt')]
    archivos_txt_ruta = [os.path.join(carpeta, f) for f in archivos_txt]
    archivos_txt_ordenados = sorted(archivos_txt_ruta, key=os.path.getsize)
    return archivos_txt_ordenados


directorio = '/opt/TelegramDownloader/descargas'
file_log = '/opt/TelegramDownloader/log.log'
mover = '/opt/TelegramDownloader/finalizado'

archivos = obtener_archivos_txt_ordenados(directorio)

for archivo in archivos:
    with open(file_log, 'a') as log:
        fecha_hora_actual = datetime.now()
        log.write(f'\n==================Inicio {archivo}=======================')
        log.write(fecha_hora_actual.strftime('%Y-%m-%d %H:%M:%S'))
    process_file_and_insert(archivo, file_log)
    with open(file_log, 'a') as log:
        fecha_hora_actual = datetime.now()
        log.write(f'\n==================Finalizado {archivo}====================')
        log.write(fecha_hora_actual.strftime('%Y-%m-%d %H:%M:%S'))

    archivo_nombre = os.path.basename(archivo)
    destino = os.path.join(mover, archivo_nombre)
    shutil.move(archivo, destino)
