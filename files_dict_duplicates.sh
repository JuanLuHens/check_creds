#!/bin/bash


directorio=${1:-$(pwd)}


declare -A hashes

echo "Buscando y borrando archivos duplicados en el directorio: $directorio"


find "$directorio" -type f | while read archivo; do

    hash=$(sha256sum "$archivo" | awk '{ print $1 }')

    if [[ -n "${hashes[$hash]}" ]]; then
        echo "Duplicado encontrado:"
        echo "  Archivo original: ${hashes[$hash]}"
        echo "  Duplicado: $archivo"
        # Borrar el archivo duplicado
        rm "$archivo"
        echo "  Borrado Duplicado"
        echo "-----------------------------------"
    else
        # Si no, almacena el hash con la ruta del archivo
        hashes[$hash]="$archivo"
    fi
done
