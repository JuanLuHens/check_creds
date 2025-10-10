from elasticsearch import Elasticsearch, helpers
import time

# Configura la conexión a OpenSearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])  # Ajusta a tu configuración

index_name = 'leak_usuarios_pro'  # Nombre del índice
file_name_value = '12321(12).txt'  # Valor del campo file_name a eliminar

# Función para borrar documentos en lotes pequeños
def delete_documents():
    # Inicializa el scroll para la búsqueda
    search_body = {
        "query": {
            "match": {
                "file_name": file_name_value  # Filtra por el campo file_name
            }
        },
        "_source": ["file_name"]  # Solo trae el campo file_name
    }

    # Se inicia la búsqueda y el scroll
    scroll = '2m'  # Define el tiempo que la búsqueda debería mantenerse abierta
    response = es.search(index=index_name, body=search_body, scroll=scroll, size=1000)  # Tamaño de los lotes de 1000
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    
    # Mientras haya documentos para eliminar
    while hits:
        # Prepara los IDs de los documentos para borrar
        actions = [{
            "_op_type": "delete",  # Operación de borrar
            "_index": index_name,
            "_id": hit['_id']  # Usar el ID del documento para borrar
        } for hit in hits]
        
        # Ejecuta el borrado de documentos en lote
        helpers.bulk(es, actions)
        
        # Obtén los siguientes documentos con el scroll
        response = es.scroll(scroll_id=scroll_id, scroll=scroll)
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        
        print(f"Documentos borrados: {len(actions)}")  # Imprime cuántos documentos se han borrado en este lote
        time.sleep(1)  # Espera 1 segundo entre lotes para no sobrecargar el servidor

    # Limpia el scroll cuando haya terminado
    es.clear_scroll(scroll_id=scroll_id)
    print("Borrado completo.")

# Llama a la función para comenzar el proceso
delete_documents()
