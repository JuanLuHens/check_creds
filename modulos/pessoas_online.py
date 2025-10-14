"""
Verificación de credenciales contra PessoasOnline (ASP.NET WebForms).

Función pública:
  login_success(usuario: str, password: str) -> bool

Comportamiento:
- Realiza GET a la página de login para obtener campos ocultos (__VIEWSTATE, etc.) y cookies
- Realiza POST simulando el clic en "Entrar" con txtLogin/txtSenha y los hidden fields
- Interpreta éxito por redirección 302 a TrocaSenha2.aspx (o presencia de parámetro auth=)
- Interpreta fallo por 200 OK manteniéndose en la página de login o por mensaje de "Login e/ou senha inválidos"

Requisitos:
  pip install requests beautifulsoup4
"""

from typing import Dict, Optional
import re
import requests
from bs4 import BeautifulSoup


LOGIN_URL = (
    "https://pessoasonline.atento.com.br/"
    "PessoasOnline/Produtos/SAAA/Principal2.aspx?amb_selecionado=0&eh_mdesigner=N&abrir_nova_janela=N&nome_portal=596E574A7469532B6372586172547958706D757270513D3D"
)

BASE_HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:142.0) Gecko/20100101 Firefox/142.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
    "Connection": "keep-alive",
}


def _extract_hidden_value(soup: BeautifulSoup, name: str) -> Optional[str]:
    el = soup.find("input", attrs={"name": name})
    if el and el.get("value") is not None:
        return el.get("value")
    return None


def _looks_like_invalid_login(html_lower: str) -> bool:
    indicators = [
        "login e/ou senha inválidos",  # mensaje explícito del sitio
        "senha inválida",
        "senha incorreta",
        "usuario ou senha",
        "usuário ou senha",
        "invalid",
        "incorrect",
        "erro",
        "error",
    ]
    return any(ind in html_lower for ind in indicators)


def login_success(usuario: str, password: str) -> bool:
    """
    Devuelve True si el login parece exitoso, False en caso contrario.

    Solo parámetros requeridos: usuario, password.
    """
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    # Paso 1: GET para obtener VIEWSTATE/VALIDATION/etc y cookies
    try:
        get_resp = session.get(LOGIN_URL, timeout=20)
    except Exception:
        return False

    if not (200 <= get_resp.status_code < 400):
        return False

    soup = BeautifulSoup(get_resp.text or "", "html.parser")

    viewstate = _extract_hidden_value(soup, "__VIEWSTATE")
    eventvalidation = _extract_hidden_value(soup, "__EVENTVALIDATION")
    viewstategen = _extract_hidden_value(soup, "__VIEWSTATEGENERATOR")

    # Paso 2: Construir payload POST simulando clic en Entrar
    payload: Dict[str, str] = {
        "__EVENTTARGET": "btnEntrar",
        "__EVENTARGUMENT": "",
        "txtLogin": usuario,
        "txtSenha": password,
    }

    # Incluir hidden fields si existen
    if viewstate is not None:
        payload["__VIEWSTATE"] = viewstate
    if viewstategen is not None:
        payload["__VIEWSTATEGENERATOR"] = viewstategen
    if eventvalidation is not None:
        payload["__EVENTVALIDATION"] = eventvalidation

    post_headers = {
        **BASE_HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://pessoasonline.atento.com.br",
        "Referer": LOGIN_URL,
    }

    # Paso 3: POST; no seguir redirecciones para inspeccionar 302/Location
    try:
        post_resp = session.post(
            LOGIN_URL,
            data=payload,
            headers=post_headers,
            timeout=20,
            allow_redirects=False,
        )
    except Exception:
        return False

    # Paso 4: Heurística de éxito/fracaso
    status = post_resp.status_code
    location = post_resp.headers.get("Location", "")
    location_lower = location.lower()

    # Éxito típico: 302 hacia TrocaSenha2.aspx (puede contener auth=)
    if 300 <= status < 400 and (
        "/pessoasonline/produtos/saaa/trocasenha2.aspx" in location_lower
        or "auth=" in location_lower
    ):
        return True

    # Si se permitió seguir redirecciones (no aquí) se podría revisar URL final.
    # En 200 OK, la página suele permanecer en Principal2.aspx y puede mostrar mensaje de inválido
    if status == 200:
        body_lower = (post_resp.text or "").lower()
        if _looks_like_invalid_login(body_lower):
            return False

        # Si no hay mensaje claro pero continúa en la misma página, interpretar como fallo
        # (buscamos pista de que seguimos en el formulario de login)
        if "id=\"form1\"" in body_lower or "name=\"form1\"" in body_lower:
            return False

    # Otros 3xx que no vayan a TrocaSenha2.aspx: conservadores -> False
    # 4xx/5xx -> False
    return False


