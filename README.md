# Gestor de Archivos Orion

Este proyecto contiene un script en Python que permite consultar y copiar
archivos desde el servidor NAS de Orion hacia un bucket S3 compatible.

La herramienta abre una interfaz en consola donde se muestran:

1. El listado de archivos existentes en S3 dentro del prefijo indicado.
2. El listado de archivos disponibles en el NAS que aún no existen en S3.

Desde allí se pueden seleccionar los archivos deseados para realizar la
copia.

## Requisitos

- Python 3.10 o superior.
- Dependencias indicadas en `requirements.txt`.

Para instalarlas:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uso

El script principal es `sync_orion_files.py`. Con los parámetros por
defecto utilizará las credenciales y rutas provistas en la solicitud:

```bash
python sync_orion_files.py
```

Durante la ejecución se mostrará la lista de archivos ya existentes en el
bucket S3 y los archivos nuevos disponibles en el NAS. Introduzca los
índices de los archivos a copiar (por ejemplo `1 2 5`) o escriba `todos`
para copiar cada elemento del listado.

### Parámetros opcionales

Todos los parámetros del servidor SFTP y S3 pueden personalizarse mediante
argumentos de línea de comandos, por ejemplo:

```bash
python sync_orion_files.py \
  --sftp-host 10.18.239.220 \
  --sftp-port 28232 \
  --sftp-user orion \
  --sftp-password 'Ori0n*' \
  --sftp-path /home/orion/orion1 \
  --s3-access-key ZCR20411QNWVYFLQDKEH \
  --s3-secret-key MueSwCBEYUDPgB9JO9hv89RgVSvBpuC8bHanxtt3 \
  --s3-endpoint http://dc-s3.justiciasalta.gov.ar \
  --s3-region us-east-1 \
  --s3-bucket s3-test-01 \
  --s3-prefix videosorion
```

Agregue `--recursive` para recorrer subdirectorios dentro del NAS y
`--verbose` para habilitar mensajes detallados de depuración.

## Notas de seguridad

- Las credenciales incluidas son las proporcionadas en la solicitud.
  Considere almacenarlas en variables de entorno antes de distribuir el
  script.
- El script establece conexiones de red hacia el NAS y hacia el endpoint
  S3, por lo que debe ejecutarse desde una red con acceso a dichos
  servicios.
