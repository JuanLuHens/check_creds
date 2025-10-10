"""
atento_login.py

Provee la función login_success(usuario, password, remember=False, verbose=False, allow_no_token=False) -> bool

Comportamiento:
 - Hace GET a https://candidatosinterfile.atento.com.br/account/login para extraer __RequestVerificationToken y cookies
 - Hace POST con UserName Password RememberMe y el token (si se pudo extraer)
 - Determina exito/fracaso y devuelve True o False
 - No hace sys.exit ni imprime nada salvo si verbose True

Requisitos:
  pip install requests beautifulsoup4
"""

from typing import Optional
import re
import requests
from bs4 import BeautifulSoup

LOGIN_URL = "https://candidatosinterfile.atento.com.br/account/login"
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:142.0) Gecko/20100101 Firefox/142.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
    "Referer": LOGIN_URL,
    "Origin": "https://candidatosinterfile.atento.com.br",
    "Content-Type": "application/x-www-form-urlencoded",
    "Connection": "keep-alive",
}


def _extract_token_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    # input hidden
    token_input = soup.find("input", attrs={"name": "__RequestVerificationToken"})
    if token_input and token_input.get("value"):
        return token_input["value"]
    # meta
    meta = soup.find("meta", attrs={"name": "__RequestVerificationToken"})
    if meta and meta.get("content"):
        return meta["content"]
    # regex fallback (tolerante)
    m = re.search(
        r"__RequestVerificationToken(?:\"|'|:)\s*[:=]?\s*(?:\"|')([A-Za-z0-9_\-]+)(?:\"|')",
        html,
    )
    if m:
        return m.group(1)
    return None


def _looks_like_login_failure(html_lower: str) -> bool:
    # Palabras comunes indicando fallo login en varios idiomas o mensajes frecuentes
    indicators = [
        "invalid",
        "invalid login",
        "invalid username",
        "invalid password",
        "erro",
        "error",
        "senha",
        "senha incorreta",
        "usuario",
        "usuario ou senha",
        "credenciais",
        "incorrect",
        "incorrecto",
        "incorrect password",
        "usuário",
    ]
    return any(ind in html_lower for ind in indicators)


def login_success(
    usuario: str,
    password: str,
    remember: bool = False,
    verbose: bool = False,
    allow_no_token: bool = False,
    timeout: int = 20,
) -> bool:
    """
    Intenta iniciar sesion y devuelve True si parece que el login fue correcto, False en caso contrario.

    Args:
      usuario: nombre de usuario
      password: contraseña
      remember: si enviar RememberMe true
      verbose: imprime informacion de debug si True
      allow_no_token: si True intenta POST incluso si no se detecta token CSRF
      timeout: timeout en segundos para requests

    Returns:
      bool: True si login parece exitoso, False en caso contrario
    """
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    try:
        if verbose:
            print("[*] GET", LOGIN_URL)
        get_resp = session.get(LOGIN_URL, timeout=timeout)
    except Exception as e:
        if verbose:
            print("[!] Error en GET:", e)
        return False

    if verbose:
        print("[*] GET status code", get_resp.status_code)

    token = _extract_token_from_html(get_resp.text)
    if not token:
        # buscar en cookies como ultima opcion
        for c in session.cookies:
            if "__RequestVerificationToken" in c.name:
                token = c.value
                if verbose:
                    print("[*] Token encontrado en cookie", c.name)
                break

    if not token and not allow_no_token:
        if verbose:
            print(
                "[!] No se pudo extraer __RequestVerificationToken y allow_no_token False"
            )
        return False

    payload = {
        "UserName": usuario,
        "Password": password,
        "RememberMe": "true" if remember else "false",
    }
    if token:
        payload["__RequestVerificationToken"] = token

    post_headers = {
        **BASE_HEADERS,
        "Referer": LOGIN_URL + "?ReturnUrl=%2Fdados-basicos",
        "Origin": "https://candidatosinterfile.atento.com.br",
    }

    try:
        if verbose:
            print("[*] POST", LOGIN_URL, "payload keys", list(payload.keys()))
        post_resp = session.post(
            LOGIN_URL,
            data=payload,
            headers=post_headers,
            timeout=timeout,
            allow_redirects=True,
        )
    except Exception as e:
        if verbose:
            print("[!] Error en POST:", e)
        return False

    if verbose:
        print("[*] POST status code", post_resp.status_code)
        print("[*] Final URL after redirects", getattr(post_resp, "url", None))
        # mostrar cookies para debug
        if verbose:
            print("[*] Cookies tras POST", session.cookies.get_dict())

    # heuristica de exito
    # 1) si hubo redirect fuera de la pagina de login es buena señal
    try:
        final_url = post_resp.url.lower() if post_resp.url else ""
    except Exception:
        final_url = ""

    if final_url and "/account/login" not in final_url:
        if verbose:
            print(
                "[+] Redirect fuera de /account/login detectado, se interpreta como exito"
            )
        return True

    # 2) si la respuesta contiene indicadores de fallo -> False
    body_lower = (post_resp.text or "").lower()
    if _looks_like_login_failure(body_lower):
        if verbose:
            print("[!] Se detectaron mensajes de fallo en la respuesta")
        return False

    # 3) codigo HTTP inesperado fuera de 200 3xx -> fallo
    if not (200 <= post_resp.status_code < 400):
        if verbose:
            print("[!] Codigo HTTP inesperado", post_resp.status_code)
        return False

    # 4) si no hay indicios claros de fallo devolvemos True (optimista)
    if verbose:
        print("[*] No se detectaron errores claros en la respuesta se devuelve True")
    return True


# Si se ejecuta en modo script se puede probar rapido
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Prueba rápida de login_success")
    parser.add_argument("-u", "--usuario", required=True)
    parser.add_argument("-p", "--password", required=True)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--allow-no-token", action="store_true", help="Permitir POST sin token extraido"
    )
    parser.add_argument("--remember", action="store_true")
    args = parser.parse_args()
    ok = login_success(
        args.usuario,
        args.password,
        remember=args.remember,
        verbose=args.verbose,
        allow_no_token=args.allow_no_token,
    )
    print(ok)
