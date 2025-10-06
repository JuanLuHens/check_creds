import requests
import json
from datetime import datetime
import urllib3
from typing import Dict, Any, List, Tuple
from modulos.config import OPENSEARCH_BASE_URL, OPENSEARCH_USER, OPENSEARCH_PASS
# Deshabilitar advertencias de SSL para conexiones internas
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = OPENSEARCH_BASE_URL
AUTH = (OPENSEARCH_USER, OPENSEARCH_PASS)

DEFAULT_INDICES = [
    "leak_atento_datos_pro",
]


def check_connection() -> bool:
    try:
        resp = requests.get(f"{BASE_URL}/_cluster/health", auth=AUTH, verify=False)
        if resp.status_code == 200:
            print("✅ Conexión exitosa a OpenSearch")
            return True
        print(f"❌ Error de conexión: {resp.status_code} - {resp.text}")
        return False
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False


def find_duplicates_in_index(index: str, page_size: int = 10000) -> List[Dict[str, Any]]:
    """
    Devuelve una lista de buckets duplicados (doc_count > 1) agrupados por url, usuario, password.
    Usa agregación compuesta para paginar todos los buckets con after_key.
    """
    duplicates: List[Dict[str, Any]] = []

    agg_body: Dict[str, Any] = {
        "size": 1000,
        "aggs": {
            "by_triplet": {
                "composite": {
                    "size": page_size,
                    "sources": [
                        {"url": {"terms": {"field": "url"}}},
                        {"usuario": {"terms": {"field": "usuario"}}},
                        {"password": {"terms": {"field": "password"}}},
                    ]
                }
            }
        }
    }

    after_key = None
    total_buckets = 0
    page_num = 0

    while True:
        page_num += 1
        body = dict(agg_body)  # copia superficial
        if after_key is not None:
            body["aggs"]["by_triplet"]["composite"]["after"] = after_key

        resp = requests.post(f"{BASE_URL}/{index}/_search", json=body, auth=AUTH, verify=False)
        if resp.status_code != 200:
            print(f"❌ Error buscando duplicados en {index}: {resp.status_code} - {resp.text}")
            break

        data = resp.json()
        buckets = data.get("aggregations", {}).get("by_triplet", {}).get("buckets", [])
        total_buckets += len(buckets)
        print(f"  Página {page_num}: {len(buckets)} buckets, total acumulado: {total_buckets}")

        for b in buckets:
            if b.get("doc_count", 0) > 1:
                key = b.get("key", {})
                duplicates.append({
                    "index": index,
                    "url": key.get("url"),
                    "usuario": key.get("usuario"),
                    "password": key.get("password"),
                    "doc_count": b.get("doc_count"),
                })

        after_key = data.get("aggregations", {}).get("by_triplet", {}).get("after_key")
        if not after_key:
            print(f"  ✅ Fin de paginación en página {page_num}")
            break

    print(f"燐 Procesados {total_buckets} grupos en {index} ({page_num} páginas). Duplicados encontrados: {len(duplicates)}")
    return duplicates


def fetch_example_docs(index: str, url: str, usuario: str, password: str, size: int = 10) -> List[Dict[str, Any]]:
    """Obtiene ejemplos de documentos para un triplete duplicado."""
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"url": url}},
                    {"term": {"usuario": usuario}},
                    {"term": {"password": password}},
                ]
            }
        },
        "size": size
    }
    resp = requests.post(f"{BASE_URL}/{index}/_search", json=query, auth=AUTH, verify=False)
    if resp.status_code != 200:
        print(f"❌ Error obteniendo ejemplos: {resp.status_code} - {resp.text}")
        return []
    data = resp.json()
    return data.get("hits", {}).get("hits", [])

# ------------------------------------
# NUEVO: utilidades de eliminación
# ------------------------------------

def fetch_all_docs_for_group(index: str, url: str, usuario: str, password: str, size: int = 1000) -> List[Dict[str, Any]]:
    """Obtiene todos los documentos de un grupo duplicado, ordenados por _id asc."""
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"url": url}},
                    {"term": {"usuario": usuario}},
                    {"term": {"password": password}},
                ]
            }
        },
        "size": size,
        "sort": [
            {"_id": {"order": "asc"}}
        ]
    }
    resp = requests.post(f"{BASE_URL}/{index}/_search", json=query, auth=AUTH, verify=False)
    if resp.status_code != 200:
        print(f"❌ Error obteniendo docs del grupo: {resp.status_code} - {resp.text}")
        return []
    return resp.json().get("hits", {}).get("hits", [])


def bulk_delete(actions: List[Tuple[str, str]]) -> bool:
    """
    Borra en bulk una lista de (index, _id).
    Devuelve True si no hay errores.
    """
    if not actions:
        return True

    # Construcción NDJSON
    lines = []
    for index, doc_id in actions:
        lines.append(json.dumps({"delete": {"_index": index, "_id": doc_id}}))
    payload = "\n".join(lines) + "\n"

    resp = requests.post(f"{BASE_URL}/_bulk", data=payload, headers={"Content-Type": "application/x-ndjson"}, auth=AUTH, verify=False)
    if resp.status_code != 200:
        print(f"❌ Error en bulk delete: {resp.status_code} - {resp.text}")
        return False

    result = resp.json()
    errors = result.get("errors", False)
    if errors:
        print("⚠️ El bulk reportó errores. Revisa la respuesta.")
    deleted = sum(1 for item in result.get("items", []) if item.get("delete", {}).get("status") in (200, 202))
    print(f"️ Eliminados {deleted}/{len(actions)} documentos")
    return not errors


def delete_duplicates_groups(dupes: List[Dict[str, Any]], batch_size: int = 1000) -> None:
    """
    Recorre los grupos duplicados y elimina todos menos el primero (por _id asc) en cada grupo.
    Envia borrados por lotes.
    """
    to_delete: List[Tuple[str, str]] = []
    total_candidates = 0

    for d in dupes:
        index = d["index"]
        url = d["url"]
        usuario = d["usuario"]
        password = d["password"]

        docs = fetch_all_docs_for_group(index, url, usuario, password)
        if len(docs) <= 1:
            continue
        # Mantener el primero, borrar el resto
        for h in docs[1:]:
            to_delete.append((index, h.get("_id")))
        total_candidates += len(docs) - 1

        # Enviar por lotes
        if len(to_delete) >= batch_size:
            print(f"Enviando borrado por lotes de {len(to_delete)} documentos...")
            ok = bulk_delete(to_delete)
            to_delete.clear()
            if not ok:
                print("⚠️ Se detectaron errores en un lote. Continuando con el siguiente lote...")

    # Último lote
    if to_delete:
        print(f"Enviando borrado final de {len(to_delete)} documentos...")
        bulk_delete(to_delete)

    print(f"\n✅ Proceso de eliminación finalizado. Candidatos borrados: {total_candidates}")

# ------------------------------------


def main():
    print(" Buscador de duplicados en OpenSearch (url, usuario, password)")
    print("=" * 70)

    if not check_connection():
        return

    indices = DEFAULT_INDICES
    print(f" Índices a analizar: {', '.join(indices)}\n")

    all_dupes: List[Dict[str, Any]] = []
    for idx in indices:
        print(f"➡️  Analizando índice: {idx}")
        dupes = find_duplicates_in_index(idx)
        all_dupes.extend(dupes)
        print()

    total = len(all_dupes)
    if total == 0:
        print("✅ No se han encontrado duplicados en los índices analizados.")
        return

    print(f"\n❗ Se han encontrado {total} grupos duplicados en total.\n")
    for i, d in enumerate(all_dupes[:50], start=1):  # mostrar hasta 50 para no saturar terminal
        print(f"[{i}] {d['index']} | count={d['doc_count']}")
        print(f"    url     : {d['url']}")
        print(f"    usuario : {d['usuario']}")
        print(f"    password: {d['password']}")
    if total > 50:
        print(f"... y {total - 50} más\n")

    # Preguntar si desea exportar
    choice = input(" ¿Exportar a JSON los duplicados? (s/n): ").strip().lower()
    if choice == 's':
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = f"duplicates_{ts}.json"
        try:
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(all_dupes, f, indent=2, ensure_ascii=False)
            print(f"✅ Exportado a {out_file}")
        except Exception as e:
            print(f"❌ Error exportando JSON: {e}")

    # Preguntar si desea ver ejemplos
    choice2 = input(" ¿Descargar ejemplos de algunos duplicados? (s/n): ").strip().lower()
    if choice2 == 's':
        num = min(5, total)
        print(f"Mostrando ejemplos de los primeros {num} grupos duplicados:\n")
        for d in all_dupes[:num]:
            docs = fetch_example_docs(d['index'], d['url'], d['usuario'], d['password'])
            print(f"== {d['index']} | {d['url']} | {d['usuario']} | count={d['doc_count']} ==")
            for h in docs:
                src = h.get('_source', {})
                print(f" - ID: {h.get('_id')}, file: {src.get('file_name')}, revisado: {src.get('revisado')}")
            print()

    # NUEVO: Preguntar si desea eliminar duplicados
    choice3 = input("️ ¿Eliminar los duplicados dejando el primero como original? (s/n): ").strip().lower()
    if choice3 == 's':
        print("\nSe eliminarán todos los documentos duplicados por grupo, conservando el primero (ordenado por _id asc).")
        confirm = input("Confirma la eliminación (escribe 'ELIMINAR'): ").strip()
        if confirm == 'ELIMINAR':
            delete_duplicates_groups(all_dupes)
        else:
            print("Operación cancelada.")


if __name__ == "__main__":
    main()
