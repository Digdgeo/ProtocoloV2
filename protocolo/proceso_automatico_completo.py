# ============================================================================
# SCRIPT COMPLETO AUTOMÁTICO: GENERAR PRODUCTOS + ENVIAR
# Versión sin confirmaciones - Ejecuta todo automáticamente
# ============================================================================

import os
import sys
import time
import glob
import shutil
import subprocess
from datetime import datetime
from pymongo import MongoClient

# Añadir ruta del código
sys.path.append('/root/git/ProtocoloV2/protocolo')
from productos import Product

try:
    from config import SSH_USER, SSH_KEY_PATH, SERVER_HOSTS
except ImportError:
    print("⚠️ No se pudo importar config.py - usando valores por defecto")
    SSH_USER = "diego_g"
    SSH_KEY_PATH = "/root/.ssh/id_rsa"
    SERVER_HOSTS = {
        "ocg.ebd.csic.es": "/var/www/html/productos_inundaciones",
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
# FASE 1: IDENTIFICAR ESCENAS
# ============================================================================

def identificar_escenas_normalizadas(umbral_nubes=20):
    """
    Identifica todas las escenas normalizadas con >umbral_nubes%.
    """
    print("\n" + "="*70)
    print(f"FASE 1: IDENTIFICANDO ESCENAS NORMALIZADAS CON >{umbral_nubes}% NUBES")
    print("="*70 + "\n")
    
    query = {'Clouds.cloud_RBIOS': {'$gt': umbral_nubes}}
    escenas = list(db.find(query, {'_id': 1, 'Clouds.cloud_RBIOS': 1}).sort('_id', 1))
    
    con_productos = []
    sin_productos = []
    
    for doc in escenas:
        escena_id = doc['_id']
        cloud_value = doc.get('Clouds', {}).get('cloud_RBIOS', 0)
        
        # Verificar normalización
        ruta_nor_esc = os.path.join(nor, escena_id)
        if not os.path.exists(ruta_nor_esc):
            continue
        
        archivos_norm = glob.glob(os.path.join(ruta_nor_esc, '*_grn2_*.tif'))
        if len(archivos_norm) < 4:
            continue
        
        # Verificar productos
        ruta_pro_esc = os.path.join(pro, escena_id, escena_id)
        
        info = {
            'escena': escena_id,
            'nubes': cloud_value,
            'bandas_norm': len(archivos_norm),
            'ruta_nor': ruta_nor_esc,
            'ruta_pro': os.path.join(pro, escena_id)
        }
        
        if os.path.exists(ruta_pro_esc):
            archivos_prod = glob.glob(os.path.join(ruta_pro_esc, '*.*'))
            if len(archivos_prod) > 0:
                info['num_productos'] = len(archivos_prod)
                con_productos.append(info)
            else:
                sin_productos.append(info)
        else:
            sin_productos.append(info)
    
    print(f"✓ Escenas normalizadas CON productos: {len(con_productos)}")
    print(f"✓ Escenas normalizadas SIN productos: {len(sin_productos)}")
    print(f"✓ TOTAL escenas a enviar: {len(con_productos) + len(sin_productos)}")
    
    return con_productos, sin_productos


# ============================================================================
# FASE 2: GENERAR PRODUCTOS FALTANTES
# ============================================================================

def generar_productos_faltantes(escenas_sin_productos):
    """
    Genera productos para las escenas que no los tienen.
    VERSIÓN AUTOMÁTICA - Sin confirmaciones
    """
    print("\n" + "="*70)
    print(f"FASE 2: GENERANDO PRODUCTOS ({len(escenas_sin_productos)} escenas)")
    print("="*70 + "\n")
    
    if len(escenas_sin_productos) == 0:
        print("✓ No hay escenas pendientes de generar productos")
        return []
    
    print("Escenas a procesar:")
    for i, esc in enumerate(escenas_sin_productos[:10], 1):
        print(f"  {i:3d}. {esc['escena']} | {esc['nubes']:.1f}%")
    if len(escenas_sin_productos) > 10:
        print(f"  ... y {len(escenas_sin_productos) - 10} más")
    
    print(f"\n🚀 Iniciando procesamiento automático de {len(escenas_sin_productos)} escenas...")
    print("="*70 + "\n")
    
    resultados = []
    exitosos = 0
    fallidos = 0
    
    inicio_total = datetime.now()
    
    for idx, esc_info in enumerate(escenas_sin_productos, 1):
        escena = esc_info['escena']
        
        print(f"\n[{idx}/{len(escenas_sin_productos)}] {escena} ({esc_info['nubes']:.1f}% nubes)")
        print("-"*70)
        
        inicio_escena = datetime.now()
        
        try:
            producto = Product(esc_info['ruta_nor'])
            producto.run()
            
            fin_escena = datetime.now()
            tiempo = (fin_escena - inicio_escena).total_seconds()
            
            print(f"  ✓ Completado en {tiempo:.1f}s")
            exitosos += 1
            
            resultados.append({
                'escena': escena,
                'exito': True,
                'tiempo': tiempo
            })
            
        except Exception as e:
            fin_escena = datetime.now()
            tiempo = (fin_escena - inicio_escena).total_seconds()
            
            print(f"  ✗ ERROR: {e}")
            fallidos += 1
            
            resultados.append({
                'escena': escena,
                'exito': False,
                'tiempo': tiempo,
                'error': str(e)
            })
        
        # Progreso cada 10
        if idx % 10 == 0 or idx == len(escenas_sin_productos):
            tiempo_transcurrido = (datetime.now() - inicio_total).total_seconds()
            promedio = tiempo_transcurrido / idx
            restantes = len(escenas_sin_productos) - idx
            estimado = promedio * restantes
            
            print(f"\n  📊 PROGRESO: {idx}/{len(escenas_sin_productos)} completadas")
            print(f"  ✓ Exitosas: {exitosos} | ✗ Fallidas: {fallidos}")
            print(f"  ⏱️  Tiempo promedio por escena: {promedio:.1f}s")
            if restantes > 0:
                print(f"  ⏱️  Tiempo estimado restante: {estimado/60:.1f} min ({estimado/3600:.2f} h)")
    
    fin_total = datetime.now()
    tiempo_total = (fin_total - inicio_total).total_seconds()
    
    print("\n" + "="*70)
    print("RESUMEN FASE 2 - GENERACIÓN DE PRODUCTOS")
    print("="*70)
    print(f"Procesadas: {len(escenas_sin_productos)}")
    print(f"Exitosas: {exitosos}")
    print(f"Fallidas: {fallidos}")
    print(f"Tiempo total: {tiempo_total/60:.1f} min ({tiempo_total/3600:.2f} h)")
    if exitosos > 0:
        print(f"Tiempo promedio: {tiempo_total/len(escenas_sin_productos):.1f}s por escena")
    
    if fallidos > 0:
        log_errores = os.path.join(path_base, 'errores_productos_batch.txt')
        with open(log_errores, 'w') as f:
            f.write(f"Errores al generar productos - {datetime.now()}\n")
            f.write("="*70 + "\n\n")
            
            print("\n⚠️ ESCENAS CON ERRORES:")
            for r in resultados:
                if not r['exito']:
                    error_msg = f"{r['escena']}: {r.get('error', 'Error desconocido')}"
                    print(f"  • {error_msg}")
                    f.write(error_msg + "\n")
        
        print(f"\n✅ Log de errores guardado en: {log_errores}")
    
    print("="*70 + "\n")
    
    return resultados


# ============================================================================
# FASE 3: ENVIAR A SERVIDORES
# ============================================================================

def enviar_a_servidores(escena_name, ruta_pro_escena):
    """
    Envía productos de una escena a los servidores remotos.
    """
    carpeta_final = os.path.join(ruta_pro_escena, escena_name)
    
    if not os.path.exists(carpeta_final):
        return False
    
    archivos_png = glob.glob(os.path.join(carpeta_final, "*.png"))
    archivos_csv = glob.glob(os.path.join(carpeta_final, "*.csv"))
    
    if not archivos_png and not archivos_csv:
        return False
    
    exitoso = True
    for host, ruta_remota in SERVER_HOSTS.items():
        try:
            ssh_user = SSH_USER if SSH_USER else "diego_g"
            ssh_key = SSH_KEY_PATH if SSH_KEY_PATH else "/root/.ssh/id_rsa"

            comando = [
                "scp", "-r",
                "-i", ssh_key,
                carpeta_final,
                f"{ssh_user}@{host}:{ruta_remota}/"
            ]
            subprocess.check_call(comando, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
        except subprocess.CalledProcessError:
            exitoso = False
        except Exception:
            exitoso = False
    
    return exitoso


def enviar_todas_las_escenas(escenas_con_productos, resultados_productos):
    """
    Envía todas las escenas normalizadas a los servidores.
    VERSIÓN AUTOMÁTICA - Sin confirmaciones
    """
    print("\n" + "="*70)
    print(f"FASE 3: ENVIANDO A SERVIDORES")
    print("="*70 + "\n")
    
    # Obtener todas las normalizadas de nuevo para tener rutas actualizadas
    con_prod, sin_prod = identificar_escenas_normalizadas(20)
    todas_normalizadas = con_prod + sin_prod
    
    print(f"Total de escenas a enviar: {len(todas_normalizadas)}")
    print(f"🚀 Iniciando envío automático a servidores...")
    print("="*70 + "\n")
    
    exitosos = 0
    fallidos = 0
    sin_archivos = 0
    
    inicio_envio = datetime.now()
    
    for idx, esc_info in enumerate(todas_normalizadas, 1):
        escena = esc_info['escena']
        ruta_pro_esc = esc_info['ruta_pro']
        
        print(f"[{idx}/{len(todas_normalizadas)}] {escena} ({esc_info['nubes']:.1f}%)", end=" ")
        
        # Verificar que existe la carpeta final
        carpeta_final = os.path.join(ruta_pro_esc, escena)
        if not os.path.exists(carpeta_final):
            print("⚠️ Sin productos")
            sin_archivos += 1
            continue
        
        archivos = glob.glob(os.path.join(carpeta_final, '*.*'))
        print(f"({len(archivos)} archivos)", end=" ")
        
        if enviar_a_servidores(escena, ruta_pro_esc):
            print("✓")
            exitosos += 1
        else:
            print("✗")
            fallidos += 1
        
        # Progreso cada 20 escenas
        if idx % 20 == 0 or idx == len(todas_normalizadas):
            tiempo_transcurrido = (datetime.now() - inicio_envio).total_seconds()
            promedio = tiempo_transcurrido / idx
            restantes = len(todas_normalizadas) - idx
            estimado = promedio * restantes
            
            print(f"\n  📊 PROGRESO: {idx}/{len(todas_normalizadas)} | ✓{exitosos} ✗{fallidos} ⚠️{sin_archivos}")
            if restantes > 0:
                print(f"  ⏱️  Tiempo estimado restante: {estimado:.0f}s\n")
    
    fin_envio = datetime.now()
    tiempo_envio = (fin_envio - inicio_envio).total_seconds()
    
    print("\n" + "="*70)
    print("RESUMEN FASE 3 - ENVÍO A SERVIDORES")
    print("="*70)
    print(f"Total: {len(todas_normalizadas)}")
    print(f"Exitosos: {exitosos}")
    print(f"Fallidos: {fallidos}")
    print(f"Sin archivos: {sin_archivos}")
    print(f"Tiempo total: {tiempo_envio:.1f}s ({tiempo_envio/60:.2f} min)")
    print("="*70 + "\n")


# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("PROCESO AUTOMÁTICO COMPLETO: PRODUCTOS + ENVÍO")
    print("="*70)
    print("\nEste script ejecutará 3 fases automáticamente:")
    print("  1. Identificar escenas normalizadas (con y sin productos)")
    print("  2. Generar productos para las que no los tienen")
    print("  3. Enviar TODAS las escenas normalizadas a servidores")
    print("\n⚠️  El proceso se ejecutará SIN CONFIRMACIONES")
    print("="*70 + "\n")
    
    inicio_global = datetime.now()
    
    # FASE 1: Identificar
    con_productos, sin_productos = identificar_escenas_normalizadas(umbral_nubes=20)
    
    # FASE 2: Generar productos faltantes
    resultados_productos = generar_productos_faltantes(sin_productos)
    
    # FASE 3: Enviar todas
    enviar_todas_las_escenas(con_productos, resultados_productos)
    
    # Resumen final
    fin_global = datetime.now()
    tiempo_global = (fin_global - inicio_global).total_seconds()
    
    print("\n" + "="*70)
    print("PROCESO COMPLETADO")
    print("="*70)
    print(f"Tiempo total del proceso: {tiempo_global/60:.1f} min ({tiempo_global/3600:.2f} h)")
    print(f"Finalizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")
