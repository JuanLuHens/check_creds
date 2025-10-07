"""
comunidad_login.py

Provee la función:
    login_success(usuario, password, *, base_url="https://comunidadatentosur.com",
                  verify_tls=True, timeout=20, verbose=False) -> bool

Comportamiento:
 - Detecta el path de login según el prefijo del usuario (PE/CL/AR/UR; case-insensitive)
 - Hace GET a la página de login para obtener cookies y posibles campos ocultos
 - Hace POST con los campos del formulario (login, password, submitAuth, _qf__formLogin + hidden)
 - Determina éxito/fracaso y devuelve True o False
 - No imprime ni hace sys.exit a menos que se ejecute como script con -v/--verbose

Requisitos:
  pip install requests beautifulsoup4
"""

from typing import Optional, Dict
import re
import requests
from bs4 import BeautifulSoup

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:142.0) Gecko/20100101 Firefox/142.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
    "Content-Type": "application/x-www-form-urlencoded",
    "Connection": "keep-alive",
}


def _login_path_for_user(usuario: str) -> str:
    pref = (usuario or "")[:2].upper()
    if pref == "PE":
        return "/peru/index.php"
    if pref == "CL":
        return "/chile/index.php"
    if pref == "AR":
        return "/argentina/index.php"
    if pref == "UR":
        return "/uruguay/index.php"
    # Fallback conservador
    return "/peru/index.php"


def _extract_hidden_inputs(html: str) -> Dict[str, str]:
    """Extrae inputs hidden del HTML de forma robusta."""
    hidden: Dict[str, str] = {}
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        for inp in soup.find_all("input", attrs={"type": "hidden"}):
            name = inp.get("name")
            if not name:
                continue
            hidden[name] = inp.get("value", "") or ""
    except Exception:
        # Fallback con regex si el HTML es raro
        for m in re.finditer(r'<input[^>]+type=["\']hidden["\'][^>]*>', html or "", flags=re.I):
            tag = m.group(0)
            nm = re.search(r'name=["\']([^"\']+)["\']', tag, flags=re.I)
            val = re.search(r'value=["\']([^"\']*)["\']', tag, flags=re.I)
            if nm:
                hidden[nm.group(1)] = val.group(1) if val else ""
    return hidden


def _looks_like_failure(html_lower: str) -> bool:
    """Heurísticos de fallo en el cuerpo."""
    indicators = [
        "user_password_incorrect",
        "loginfailed",
        "error=user_password_incorrect",
        "invalid",
        "incorrect",
        "contraseña",  # por si el sitio cambia textos
        "contrasena",
    ]
    return any(ind in html_lower for ind in indicators)


def login_success(
    usuario: str,
    password: str,
    *,
    base_url: str = "https://comunidadatentosur.com",
    verify_tls: bool = True,
    timeout: int = 20,
    verbose: bool = False,
) -> bool:
    """
    Intenta iniciar sesión y devuelve True si el login es correcto, False si no.

    Args:
      usuario: login (ej. PE..., CL..., AR..., UR...)
      password: contraseña
      base_url: origen del sitio (por defecto https://comunidadatentosur.com)
      verify_tls: verificar certificados TLS (True por defecto)
      timeout: segundos de timeout por request
      verbose: logs de depuración por stdout

    Returns:
      bool
    """
    login_path = _login_path_for_user(usuario)
    login_url = base_url.rstrip("/") + login_path

    headers = {
        **BASE_HEADERS,
        "Origin": base_url,
        "Referer": login_url,
    }

    s = requests.Session()
    s.headers.update(headers)

    # 1) GET inicial para cookies + hidden inputs
    try:
        if verbose:
            print(f"[*] GET {login_url}")
        r_get = s.get(login_url, timeout=timeout, verify=verify_tls)
        if verbose:
            print(f"[*] GET status {r_get.status_code}")
    except requests.RequestException as e:
        if verbose:
            print(f"[!] Error GET: {e}")
        return False

    hidden = _extract_hidden_inputs(r_get.text)

    # 2) Construir payload del formulario
    payload = {
        **hidden,
        "login": usuario,
        "password": password,
        "submitAuth": "",
        "_qf__formLogin": "",
    }

    # 3) POST (sin seguir redirecciones para inspeccionar Location)
    try:
        if verbose:
            ks = ", ".join(payload.keys())
            print(f"[*] POST {login_url} (keys: {ks})")
        r_post = s.post(
            login_url,
            data=payload,
            allow_redirects=False,
            timeout=timeout,
            verify=verify_tls,
        )
        if verbose:
            print(f"[*] POST status {r_post.status_code}")
            print(f"[*] Location: {r_post.headers.get('Location')}")
    except requests.RequestException as e:
        if verbose:
            print(f"[!] Error POST: {e}")
        return False

    # 4) Reglas de decisión
    # 4.a) Si hay Location con marcador conocido de fallo -> False
    loc = (r_post.headers.get("Location") or "").lower()
    if any(x in loc for x in ("user_password_incorrect", "loginfailed", "error=user_password_incorrect")):
        if verbose:
            print("[!] Location indica fallo de credenciales")
        return False

    # 4.b) Si es 302 y no es fallo conocido, asumimos éxito
    if r_post.status_code == 302:
        if verbose:
            print("[+] 302 sin indicador de error: se interpreta como éxito")
        return True

    # 4.c) Revisar el cuerpo por indicadores de error
    body_lower = (r_post.text or "").lower()
    if _looks_like_failure(body_lower):
        if verbose:
            print("[!] Cuerpo indica fallo de login")
        return False

    # 4.d) Señales de sesión iniciada
    if any(k in body_lower for k in ("logout", "cerrar sesion", "mi cuenta", "dashboard", "perfil")):
        if verbose:
            print("[+] Indicadores de sesión iniciada en el cuerpo")
        return True

    # 4.e) Fallback optimista (no se detectó error explícito)
    if verbose:
        print("[*] No hay señales claras de error; se devuelve True (optimista)")
    return True


# Bloque de prueba rápida (opcional). No afecta al uso como módulo.
if __name__ == "__main__":
    import argparse, sys

    ap = argparse.ArgumentParser(description="Prueba rápida de login en comunidadatentosur.com")
    ap.add_argument("-u", "--usuario", required=True)
    ap.add_argument("-p", "--password", required=True)
    ap.add_argument("--base-url", default="https://comunidadatentosur.com")
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
    # No imprimimos el booleano salvo que el usuario pida verbose; solo salimos con código
    if args.verbose:
        print("Resultado:", ok)
    sys.exit(0 if ok else 1)

