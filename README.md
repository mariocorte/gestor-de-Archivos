# Gestor de Archivos Orion

Este proyecto proporciona una herramienta de sincronización entre un servidor
SFTP y un bucket de Amazon S3 junto con una interfaz web ligera que permite
lanzar el proceso desde el navegador. Está pensado para manejar nombres de
archivos con caracteres especiales (por ejemplo, `Ñ`) sin provocar errores de
codificación.

## Requisitos

- Python 3.11 o superior
- Dependencias listadas en `requirements.txt`

Instala las dependencias con:

```bash
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configuración

La herramienta utiliza variables de entorno para conocer los parámetros de
conexión y el destino en S3. Puedes definirlas manualmente o a través de un
archivo `.env` (si usas `python-dotenv`).

Variables principales:

- `SFTP_HOST`: host del servidor SFTP.
- `SFTP_PORT`: puerto (por defecto `22`).
- `SFTP_USERNAME` y `SFTP_PASSWORD`: credenciales de acceso.
- `SFTP_PRIVATE_KEY`: ruta a la clave privada (opcional, alternativa a la contraseña).
- `SFTP_PASSPHRASE`: passphrase de la clave privada si aplica.
- `SFTP_BASE_PATH`: carpeta inicial a sincronizar.
- `SFTP_ENCODINGS`: lista separada por comas con codificaciones a probar (`utf-8,latin-1,cp1252`).
- `S3_BUCKET`: bucket de destino.
- `S3_PREFIX`: prefijo (carpeta lógica) dentro del bucket.
- `AWS_REGION`: región de AWS (opcional).
- `DELETE_REMOTE_AFTER_UPLOAD`: `true/false` para eliminar el archivo remoto tras subirlo.
- `ALLOWED_EXTENSIONS`: lista separada por comas con extensiones permitidas (por ejemplo `mp4,mov`).

## Uso por línea de comandos

Carga las variables de entorno y ejecuta:

```bash
python sync_orion_files.py --env-file .env --verbose
```

Parámetros disponibles:

- `--env-file`: carga un archivo `.env` con la configuración.
- `--dry-run`: simula la sincronización sin subir archivos.
- `--list-only`: solo lista los archivos detectados.
- `--verbose`: muestra información adicional en los logs.

## Interfaz web

Para lanzar la aplicación web ejecuta:

```bash
flask --app webapp run
```

La interfaz estará disponible en `http://127.0.0.1:5000/`. Completa el
formulario con los parámetros necesarios y pulsa **Ejecutar sincronización**.
El panel mostrará los registros y el resumen de la ejecución.

> **Nota:** la aplicación reutiliza la misma lógica que la herramienta CLI, por
> lo que respeta las opciones de codificación para evitar errores de
> `UnicodeDecodeError` al recorrer el SFTP.

## Pruebas

Puedes verificar rápidamente que el código es válido ejecutando:

```bash
python -m compileall sync_orion_files.py webapp.py
```

Esto compila los módulos y ayuda a detectar errores de sintaxis.
