#!/bin/bash

# Establecer la ruta de tu instalación de Anaconda
CONDA_PATH="/home/usuario/anaconda3"  # Cambiar "usuario" a nombre real de usuario
ENV_NAME="pv2"  # Nombre del entorno en Anaconda

# Ruta al proyecto y archivo de logs
PROJECT_PATH="/ruta/a/proyecto"
LOG_FILE="/ruta/a/proyecto/logs/descarga_semanal.log"

# Añadir Anaconda a PATH
export PATH="$CONDA_PATH/bin:$PATH"

# Activar el entorno de Anaconda
source "$CONDA_PATH/bin/activate" $ENV_NAME

# Ejecutar el script de descarga y procesado
cd $PROJECT_PATH
python download.py --username USUARIO --password CONTRASEÑA --latitude 37.05 --longitude -6.35 --days_back 7 --output_dir "/ruta/de/salida" >> $LOG_FILE 2>&1

# Desactivar el entorno de Anaconda
conda deactivate
