#!/bin/bash

# Establecer la ruta de tu instalación de Anaconda (cambia esto si no está en /home/usuario/anaconda3)
CONDA_PATH="/home/usuario/anaconda3"  # Cambiar "usuario" a nombre real de usuario
ENV_NAME="pv2"  # Nombre del entorno en Anaconda

# Ruta al proyecto y archivo de logs
PROJECT_PATH="/ruta/a/proyecto"  # Ruta donde está tu proyecto, utils.py e hidroperiodo.py
LOG_FILE="/ruta/a/proyecto/logs/hidroperiodo_anual.log"  # Archivo de logs

# Añadir Anaconda a PATH (necesario en algunos casos)
export PATH="$CONDA_PATH/bin:$PATH"

# Activar el entorno de Anaconda
source "$CONDA_PATH/bin/activate" $ENV_NAME

# Calcular el año actual y el año anterior
YEAR_CURRENT=$(date +'%Y')  # Año actual
YEAR_PREVIOUS=$((YEAR_CURRENT - 1))  # Año anterior

# Definir el ciclo hidrológico basado en los años
CICLO_HIDROLOGICO="${YEAR_PREVIOUS}-${YEAR_CURRENT}"

# Ejecutar prepare_hydrop de utils.py con los años calculados
cd $PROJECT_PATH
python -c "from utils import prepare_hydrop; prepare_hydrop('/ruta/productos/', '/ruta/salida/', '$CICLO_HIDROLOGICO', 20)" >> $LOG_FILE 2>&1

# Ejecutar las funciones de hidroperiodo.py
python -c "from hidroperiodo import get_escenas_values, get_hydroperiod, get_products; 
escenas_values = get_escenas_values('/ruta/escenas/');
get_hydroperiod('/ruta/escenas/', escenas_values);
get_products('/ruta/output/')" >> $LOG_FILE 2>&1

# Desactivar el entorno de Anaconda
conda deactivate

#chmod +x /ruta/a/tu/proyecto/run_hydroperiod.sh ##Y run_download.sh
#crontab -e
#0 3 * * 0 /ruta/a/tu/proyecto/run_download.sh ##protocolov2
#0 2 15 10 * /ruta/a/tu/proyecto/run_hydroperiod.sh
