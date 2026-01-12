# ============================================================================
# ENVÍO DE ESCENAS LANDSAT CON >30% NUBES YA NORMALIZADAS
# ============================================================================

import os
import sys
import glob
import shutil
import subprocess
from datetime import datetime
from pymongo import MongoClient

# Añadir ruta del código para importar configuración
sys.path.append('/root/git/ProtocoloV2/protocolo')
try:
    from config import SSH_USER, SSH_KEY_PATH, SERVER_HOSTS
except ImportError:
    print("⚠️ No se pudo importar config.py - usando valores por defecto")
    SSH_USER = "diego_g"
    SSH_KEY_PATH = "/root/.ssh/id_rsa"
    SERVER_HOSTS = {
        "ocg.ebd.csic.es": "/var/www/html/productos_inundaciones",
        "vps": "/ruta/a/productos"
    }

# Configuración de MongoDB
client = MongoClient()
database = client.Satelites
db = database.Landsat

# Configuración de rutas
path_base = '/mnt/datos_last/'
ori = os.path.join(path_base, 'ori')
nor = os.path.join(path_base, 'nor')
pro = os.path.join(path_base, 'pro')

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def verificar_normalizacion(escena_name, ruta_nor):
    """
    Verifica que una escena esté normalizada.
    Retorna True si tiene archivos .tif normalizados, False en caso contrario.
    """
    ruta_completa = os.path.join(ruta_nor, escena_name)
    
    if not os.path.exists(ruta_completa):
        return False
    
    # Buscar archivos .tif (excluyendo fmask y hillshade)
    archivos_tif = glob.glob(os.path.join(ruta_completa, '*_grn2_*.tif'))
    
    # Debe tener al menos 4 bandas normalizadas
    return len(archivos_tif) >= 4


def copiar_productos_a_servidores(escena_name, ruta_pro_escena):
    """
    Copia los productos de una escena a los servidores remotos.
    Replica la lógica de movidas_de_servidores() de productos.py
    """
    # Crear carpeta final con el nombre de la escena dentro de pro_escena
    carpeta_final = os.path.join(ruta_pro_escena, escena_name)
    
    # Verificar que exista la carpeta con productos
    if not os.path.exists(carpeta_final):
        print(f"  ⚠️ No existe carpeta de productos: {carpeta_final}")
        return False
    
    # Verificar que haya archivos para enviar
    archivos_png = glob.glob(os.path.join(carpeta_final, "*.png"))
    archivos_csv = glob.glob(os.path.join(carpeta_final, "*.csv"))
    
    if not archivos_png and not archivos_csv:
        print(f"  ⚠️ No hay archivos PNG/CSV para enviar en {carpeta_final}")
        return False
    
    print(f"  📦 Archivos a enviar: {len(archivos_png)} PNG, {len(archivos_csv)} CSV")
    
    # Enviar a cada servidor
    exitoso = True
    for host, ruta_remota in SERVER_HOSTS.items():
        try:
            ssh_user = SSH_USER if SSH_USER else "diego_g"
            ssh_key = SSH_KEY_PATH if SSH_KEY_PATH else "/root/.ssh/id_rsa"

            print(f"  📤 Copiando a {host} como usuario {ssh_user}...")
            comando = [
                "scp", "-r",
                "-i", ssh_key,
                carpeta_final,
                f"{ssh_user}@{host}:{ruta_remota}/"
            ]
            subprocess.check_call(comando)
            print(f"  ✓ Copia completada en {host}")
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Error al copiar a {host}: {e}")
            exitoso = False
        except Exception as e:
            print(f"  ✗ Error inesperado con {host}: {e}")
            exitoso = False
    
    return exitoso


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def buscar_y_enviar_escenas_nubosas(umbral_nubes=30, anios=None, modo_prueba=True):
    """
    Busca en MongoDB escenas con cloud_RBIOS > umbral_nubes que estén normalizadas
    y las envía a los servidores.
    
    Args:
        umbral_nubes: Porcentaje mínimo de nubes (default: 30)
        anios: Lista de años a procesar (None = todos)
        modo_prueba: Si True, solo muestra lo que haría sin enviar
    """
    print("\n" + "="*70)
    print(f"BÚSQUEDA DE ESCENAS NORMALIZADAS CON >{umbral_nubes}% DE NUBES EN RBIOS")
    if modo_prueba:
        print("⚠️ MODO PRUEBA ACTIVADO - No se enviarán archivos")
    print("="*70 + "\n")

    # Validar directorios
    if not os.path.exists(nor):
        print(f"ERROR: No existe {nor}")
        return
    if not os.path.exists(pro):
        print(f"ERROR: No existe {pro}")
        return

    # Consultar MongoDB
    print(f"🔍 Consultando MongoDB...")
    
    # Construir query
    query = {
        'Clouds.cloud_RBIOS': {'$gt': umbral_nubes}
    }
    
    # Filtrar por años si se especifica
    if anios:
        regex_pattern = '|'.join([f'^{str(anio)}' for anio in anios])
        query['_id'] = {'$regex': regex_pattern}
    
    # Proyección para obtener solo los campos necesarios
    projection = {
        '_id': 1,
        'Clouds.cloud_RBIOS': 1,
        'usgs_id': 1
    }
    
    try:
        escenas_cursor = db.find(query, projection).sort('_id', 1)
        escenas_candidatas = list(escenas_cursor)
    except Exception as e:
        print(f"ERROR al consultar MongoDB: {e}")
        return
    
    print(f"📊 Escenas encontradas en MongoDB: {len(escenas_candidatas)}\n")
    
    if len(escenas_candidatas) == 0:
        print("No se encontraron escenas que cumplan los criterios")
        return

    # Filtrar por normalización
    escenas_validas = []
    
    for doc in escenas_candidatas:
        escena_id = doc['_id']
        cloud_value = doc.get('Clouds', {}).get('cloud_RBIOS', None)
        
        if cloud_value is None:
            print(f"⚠️ {escena_id}: sin dato cloud_RBIOS")
            continue
        
        # Verificar normalización
        if verificar_normalizacion(escena_id, nor):
            escenas_validas.append({
                'escena': escena_id,
                'nubes': cloud_value,
                'usgs_id': doc.get('usgs_id', 'N/A')
            })
            print(f"✓ {escena_id} | {cloud_value:.1f}% nubes | Normalizada")
        else:
            print(f"✗ {escena_id} | {cloud_value:.1f}% nubes | NO normalizada")

    # Resumen
    print(f"\n{'='*70}")
    print(f"ESCENAS VÁLIDAS (normalizadas con >{umbral_nubes}% nubes): {len(escenas_validas)}")
    print(f"{'='*70}\n")

    if len(escenas_validas) == 0:
        print("No hay escenas normalizadas para enviar")
        return

    # Mostrar lista ordenada
    escenas_ordenadas = sorted(escenas_validas, key=lambda x: x['nubes'], reverse=True)
    
    print("Lista de escenas a enviar (ordenadas por % nubes):\n")
    for i, esc in enumerate(escenas_ordenadas, 1):
        print(f"  {i:3d}. {esc['escena']} | {esc['nubes']:5.1f}% | {esc['usgs_id']}")

    # Confirmar envío
    if not modo_prueba:
        print(f"\n{'='*70}")
        respuesta = input(f"¿Enviar {len(escenas_validas)} escenas a los servidores? (si/no): ")
        if respuesta.lower() not in ['si', 's', 'yes', 'y']:
            print("Operación cancelada")
            return

    # Procesar envíos
    print(f"\n{'='*70}")
    print("INICIANDO ENVÍOS")
    print(f"{'='*70}\n")
    
    exitosos = 0
    fallidos = 0
    sin_productos = 0
    
    for i, esc in enumerate(escenas_ordenadas, 1):
        escena_name = esc['escena']
        ruta_pro_escena = os.path.join(pro, escena_name)
        
        print(f"\n[{i}/{len(escenas_validas)}] {escena_name} ({esc['nubes']:.1f}% nubes)")
        
        if modo_prueba:
            # En modo prueba, verificar si existen productos
            carpeta_final = os.path.join(ruta_pro_escena, escena_name)
            if os.path.exists(carpeta_final):
                archivos = glob.glob(os.path.join(carpeta_final, "*.*"))
                print(f"  🔍 MODO PRUEBA: Se enviarían {len(archivos)} archivos")
                exitosos += 1
            else:
                print(f"  ⚠️ MODO PRUEBA: No hay productos generados")
                sin_productos += 1
        else:
            # Envío real
            if copiar_productos_a_servidores(escena_name, ruta_pro_escena):
                exitosos += 1
            else:
                sin_productos += 1

    # Resumen final
    print(f"\n{'='*70}")
    print("RESUMEN FINAL")
    print(f"{'='*70}")
    print(f"Total procesadas: {len(escenas_validas)}")
    print(f"Exitosas: {exitosos}")
    print(f"Sin productos: {sin_productos}")
    print(f"Fallidas: {fallidos}")
    print(f"{'='*70}\n")


def listar_escenas_por_anio(umbral_nubes=30):
    """
    Lista las escenas con >umbral_nubes% agrupadas por año.
    Útil para decidir qué años procesar.
    """
    print("\n" + "="*70)
    print(f"LISTADO DE ESCENAS CON >{umbral_nubes}% NUBES POR AÑO")
    print("="*70 + "\n")
    
    query = {'Clouds.cloud_RBIOS': {'$gt': umbral_nubes}}
    projection = {'_id': 1, 'Clouds.cloud_RBIOS': 1}
    
    try:
        escenas = list(db.find(query, projection).sort('_id', 1))
    except Exception as e:
        print(f"ERROR: {e}")
        return
    
    # Agrupar por año
    por_anio = {}
    for doc in escenas:
        escena_id = doc['_id']
        anio = escena_id[:4]  # Primeros 4 caracteres = año
        cloud_value = doc.get('Clouds', {}).get('cloud_RBIOS', 0)
        
        if anio not in por_anio:
            por_anio[anio] = []
        por_anio[anio].append({
            'escena': escena_id,
            'nubes': cloud_value
        })
    
    # Mostrar resumen
    for anio in sorted(por_anio.keys()):
        escenas_anio = por_anio[anio]
        promedio = sum(e['nubes'] for e in escenas_anio) / len(escenas_anio)
        print(f"{anio}: {len(escenas_anio):3d} escenas | Promedio nubes: {promedio:.1f}%")
    
    print(f"\n{'='*70}")
    print(f"TOTAL: {len(escenas)} escenas en {len(por_anio)} años")
    print(f"{'='*70}\n")


# ============================================================================
# EJECUCIÓN
# ============================================================================

if __name__ == "__main__":
    
    # CONFIGURACIÓN
    # -------------
    
    # Porcentaje mínimo de nubes
    UMBRAL_NUBES = 30
    
    # Años a procesar (None = todos los años, o lista específica)
    # ANIOS = None  # Todos
    ANIOS = [1984, 1985, 1990, 2000]  # Lista específica
    
    # Modo prueba (True = solo muestra, False = envía realmente)
    MODO_PRUEBA = True
    
    # OPCIONES DE EJECUCIÓN
    # ---------------------
    
    # Opción 1: Listar escenas por año (para decidir qué procesar)
    # listar_escenas_por_anio(umbral_nubes=UMBRAL_NUBES)
    
    # Opción 2: Buscar y enviar escenas
    buscar_y_enviar_escenas_nubosas(
        umbral_nubes=UMBRAL_NUBES,
        anios=ANIOS,
        modo_prueba=MODO_PRUEBA
    )
