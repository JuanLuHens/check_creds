#!/bin/bash

script_name=$(basename "$0")

# Comprobar si ya hay una instancia en ejecución (excepto el proceso actual)
if pgrep -f "$script_name" | grep -v $$ > /dev/null; then
    echo "El script ya está en ejecución."
    exit 1
fi
echo "Se encontraron archivos .txt. Ejecutando script Python..."
VIRTUAL_ENV="/home/redteam/check_creds/venv"
source "$VIRTUAL_ENV/bin/activate"
python3 /home/redteam/check_creds/check_creds.py
