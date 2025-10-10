import argparse
import sys
import time
from typing import Dict, Any

import requests
import urllib3

from modulos.config import (
    OPENSEARCH_BASE_URL,
    OPENSEARCH_USER,
    OPENSEARCH_PASS,
)


def build_query(file_name: str, use_match: bool) -> Dict[str, Any]:
    if use_match:
        return {"query": {"match": {"file_name": file_name}}}
    # Por defecto: coincidencia exacta usando el campo keyword
    return {"query": {"term": {"file_name.keyword": file_name}}}


def count_docs(index: str, body: Dict[str, Any], auth, verify: bool, timeout: int) -> int:
    base = OPENSEARCH_BASE_URL.rstrip('/')
    url = f"{base}/{index}/_count"
    r = requests.post(url, json=body, auth=auth, verify=verify, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return int(data.get("count", 0))


def delete_by_query(index: str, body: Dict[str, Any], auth, verify: bool, timeout: int, wait: bool):
    base = OPENSEARCH_BASE_URL.rstrip('/')
    url = f"{base}/{index}/_delete_by_query"
    params = {
        "conflicts": "proceed",
        "slices": "auto",
        "wait_for_completion": str(wait).lower(),
    }
    r = requests.post(url, params=params, json=body, auth=auth, verify=verify, timeout=timeout)
    r.raise_for_status()
    return r.json()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Borra documentos de OpenSearch por file_name usando _delete_by_query.")
    parser.add_argument("--index", required=True, help="Nombre del índice")
    parser.add_argument("--file-name", required=True, help="Valor exacto de file_name a borrar")
    parser.add_argument("--confirm", action="store_true", help="Ejecutar el borrado real (sin solo previsualizar)")
    parser.add_argument("--dry-run", action="store_true", help="No borrar; solo mostrar cuántos documentos coinciden")
    parser.add_argument("--wait", action="store_true", help="Esperar a que termine el _delete_by_query")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout en segundos para las peticiones HTTP (por defecto 120)")
    parser.add_argument("--insecure", action="store_true", help="No verificar el certificado TLS (verify=False)")
    parser.add_argument("--match", action="store_true", help="Usar match en lugar de term(file_name.keyword)")
    return parser.parse_args()


def main():
    args = parse_args()

    if not OPENSEARCH_BASE_URL:
        print("ERROR: OPENSEARCH_BASE_URL no está configurado en el entorno (.env).", file=sys.stderr)
        sys.exit(2)

    auth = (OPENSEARCH_USER, OPENSEARCH_PASS) if OPENSEARCH_USER else None
    verify = not args.insecure
    if not verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    body = build_query(args["file_name" if False else "file_name"], args.match)  # mantén nombre claro
    # La línea anterior asegura nombre del argumento; acceso directo más abajo para claridad
    body = build_query(args.file_name, args.match)

    # Dry-run o confirmación previa: contar documentos afectados
    try:
        num = count_docs(args.index, body, auth, verify, args.timeout)
    except requests.HTTPError as e:
        print(f"Error al contar documentos: {e} - {getattr(e.response, 'text', '')}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error al contar documentos: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Índice: {args.index}")
    print(f"Criterio: {'match(file_name)' if args.match else 'term(file_name.keyword)'} == '{args.file_name}'")
    print(f"Coincidencias encontradas: {num}")

    if args.dry_run:
        print("Dry-run: no se realizará el borrado.")
        return

    if not args.confirm:
        print("Falta --confirm. Ejecuta con --confirm para proceder al borrado.")
        return

    if num == 0:
        print("No hay documentos que borrar.")
        return

    t0 = time.time()
    try:
        result = delete_by_query(args.index, body, auth, verify, args.timeout, args.wait)
    except requests.HTTPError as e:
        print(f"Error en _delete_by_query: {e} - {getattr(e.response, 'text', '')}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error en _delete_by_query: {e}", file=sys.stderr)
        sys.exit(1)

    elapsed = time.time() - t0
    if args.wait:
        deleted = result.get("deleted")
        version_conflicts = result.get("version_conflicts")
        failures = result.get("failures", [])
        print(f"Borrado completado en {elapsed:.1f}s. Eliminados: {deleted}. Conflictos: {version_conflicts}.")
        if failures:
            print(f"Fallos: {len(failures)}")
    else:
        task = result.get("task")
        print(f"Borrado iniciado (no se espera finalización). Task id: {task}")


if __name__ == "__main__":
    main()
