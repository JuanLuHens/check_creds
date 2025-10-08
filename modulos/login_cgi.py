
"""
portalcgi_login.py

Provee la función:
    login_success(usuario, password, *, base_url="https://portalcgi.atento.com.br",
                  verify_tls=True, timeout=20, verbose=False) -> bool

Comportamiento:
 - Hace GET a /portal/index.php para obtener cookies (PHPSESSID, TS...)
 - Hace POST AJAX a /portal/aj.php con los campos (aj, app, login, senha, n_action, etc.)
 - Si el body contiene 'senha expirada' (PT) interpreta credencial válida pero caducada => True
 - Si el body contiene 'Usuário ou senha inválida!' (u otras variantes) => False
 - Determina éxito/fracaso y devuelve True o False
 - No imprime ni hace sys.exit a menos que se ejecute como script con -v/--verbose

Requisitos:
  pip install requests beautifulsoup4
"""

from typing import Dict
import re
import requests
from bs4 import BeautifulSoup  # mantenido por homogeneidad del formato

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:142.0) Gecko/20100101 Firefox/142.0",
    "Accept": "*/*",
    "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Connection": "keep-alive",
}

INDEX_PATH = "/portal/index.php"
AJ_PATH    = "/portal/aj.php"


def _extract_cookie_pairs(set_cookie_header: str) -> Dict[str, str]:
    """Extrae pares clave=valor simples de un header Set-Cookie (best-effort)."""
    cookies: Dict[str, str] = {}
    for m in re.finditer(r'([^=;,\s]+)=([^;,\s]+)', set_cookie_header or ""):
        k, v = m.group(1), m.group(2)
        if k.lower() not in ("path", "expires", "domain", "secure", "httponly", "samesite"):
            cookies.setdefault(k, v)
    return cookies


def _looks_like_expired(html: str) -> bool:
    """Detecta mensajes de contraseña expirada (tratar como éxito True)."""
    s = (html or "").lower()
    # Variantes comunes con y sin acentos / espacios
    indicators = [
        "senha expirada",
        "sua senha expirou",
        "senha esta expirada",
        "senha expirou",  # tu caso exacto
    ]
    return any(ind in s for ind in indicators)


def _looks_like_failure(html: str) -> bool:
    """Detecta fallos conocidos (PT/ES/EN) de credenciales inválidas."""
    s = (html or "").lower()
    # Mensaje exacto reportado (con y sin acentos)
    indicators = [
        "usuário ou senha inválida",
        "usuario ou senha invalida",
        "senha invalida",
        "senha incorreta",
        "usuario invalido",
        "login incorreto",
        "acesso negado",
        "forbidden",
        "invalid",
        "incorrect",
        "error",
        "failed",
        "falha",
        "erro",
        "credencial",
        "nao encontrado",
        "não encontrado",
    ]
    return any(ind in s for ind in indicators)


def login_success(
    usuario: str,
    password: str,
    *,
    base_url: str = "https://portalcgi.atento.com.br",
    verify_tls: bool = True,   # True | False | "/ruta/a/ca.crt"
    timeout: int = 20,
    verbose: bool = False,
) -> bool:
    """
    Intenta iniciar sesión en portalcgi.atento.com.br y devuelve True si el login es correcto, False si no.
    - Trata 'senha expirada' como True (credencial válida pero caducada).
    """
    base = base_url.rstrip("/")
    index_url = base + INDEX_PATH
    aj_url    = base + AJ_PATH

    # 1) Sesión + headers base
    s = requests.Session()
    headers_get = {
        **BASE_HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": index_url,
    }
    headers_post = {
        **BASE_HEADERS,
        "Origin": base,
        "Referer": index_url,
        "X-Requested-With": "XMLHttpRequest",
    }

    # 2) GET inicial para obtener cookies (PHPSESSID, TS...)
    try:
        if verbose:
            print(f"[*] GET {index_url}")
        r_get = s.get(index_url, headers=headers_get, timeout=timeout, verify=verify_tls)
        if verbose:
            print(f"[*] GET status {r_get.status_code}")
    except requests.RequestException as e:
        if verbose:
            print(f"[!] Error GET: {e}")
        return False

    # Mezclar cookies por si vienen en Set-Cookie y en cookiejar
    cookies = {c.name: c.value for c in s.cookies}
    sc = r_get.headers.get("Set-Cookie", "") or ""
    cookies.update(_extract_cookie_pairs(sc))

    if verbose and cookies:
        print("[*] Cookies vistas tras GET:")
        for k, v in cookies.items():
            print(f"    {k} = {v}")

    # 3) Payload de login (según tu ejemplo)
    payload = {
        "aj": "login",
        "app": "core",
        "login": usuario,
        "senha": password,
        "n_action": "login",
        "n_senha": "",
        "n_senha_c": "",
        "AD_ID": "",
    }

    # 4) POST AJAX (sin seguir redirecciones)
    try:
        if verbose:
            print(f"[*] POST {aj_url} (keys: {', '.join(payload.keys())})")
        r_post = s.post(
            aj_url,
            data=payload,
            headers=headers_post,
            allow_redirects=False,
            timeout=timeout,
            verify=verify_tls,
        )
        if verbose:
            print(f"[*] POST status {r_post.status_code}")
            loc = r_post.headers.get("Location")
            if loc:
                print(f"[*] Location: {loc}")
    except requests.RequestException as e:
        if verbose:
            print(f"[!] Error POST: {e}")
        return False

    # 5) Reglas de decisión
    body = r_post.text or ""

    # a) Si la contraseña está expirada, tratamos como éxito (credencial correcta pero caducada)
    if _looks_like_expired(body):
        if verbose:
            print("[+] Señal de 'senha expirada' detectada: interpretado como éxito")
        return True

    # b) Si el cuerpo indica fallo claro de credenciales -> False
    if _looks_like_failure(body):
        if verbose:
            print("[!] Cuerpo indica fallo de login")
        return False

    # c) Redirect 302/303 suele indicar éxito
    if r_post.status_code in (302, 303):
        if verbose:
            print("[+] 302/303 detectado: interpretado como éxito")
        return True

    # d) Otras señales de sesión iniciada en el cuerpo (por si responde HTML)
    bl = body.lower()
    if any(k in bl for k in ("logout", "sair", "minha conta", "perfil", "dashboard", "inicio")):
        if verbose:
            print("[+] Indicadores de sesión iniciada en el cuerpo")
        return True

    # e) Fallback optimista: si no hay error explícito, consideramos True
    if verbose:
        print("[*] Sin error explícito; devolviendo True")
    return True


# Bloque de prueba rápida (opcional). No afecta al uso como módulo.
if __name__ == "__main__":
    import argparse, sys

    ap = argparse.ArgumentParser(description="Prueba rápida de login en portalcgi.atento.com.br")
    ap.add_argument("-u", "--usuario", required=True)
    ap.add_argument("-p", "--password", required=True)
    ap.add_argument("--base-url", default="https://portalcgi.atento.com.br")
    ap.add_argument("--insecure", action="store_true", help="No verificar TLS")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    ok = login_success(
        args.usuario,
        args.password,
        base_url=args.base_url,
        verify_tls=not args.insecure,
        timeout=args.timeout,
        verbose=args.verbose,
    )
    if args.verbose:
        print("Resultado:", ok)
    sys.exit(0 if ok else 1)

