
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Regenera productos en paralelo desde escenas ya normalizadas.
"""

import os
import sys
from datetime import datetime
from multiprocessing import Pool
import traceback

sys.path.append('/root/git/ProtocoloV2/protocolo')
from productos import Product

# Configuración
NOR_PATH = '/mnt/datos_last/nor'
FECHA_INICIO = datetime(2024, 12, 1)
FECHA_FIN = datetime(2025, 8, 31)
NUM_WORKERS = 4  # Ajustar según recursos

def extraer_fecha(nombre_escena):
    """Extrae la fecha del nombre de la escena normalizada."""
    try:
        fecha_str = nombre_escena[:8]
        return datetime.strptime(fecha_str, '%Y%m%d')
    except:
        return None

def procesar_escena(args):
    """Procesa una escena (para multiprocessing)."""
    escena, ruta_nor = args
    
    try:
        product = Product(ruta_nor)
        product.run()
        return (escena, True, None)
        
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        return (escena, False, error_msg)

def main():
    """Función principal."""
    
    print(f"\n{'='*80}")
    print("REGENERAR PRODUCTOS PARALELO - CICLO 2024/25")
    print(f"Workers: {NUM_WORKERS}")
    print(f"Desde: {FECHA_INICIO.strftime('%d/%m/%Y')}")
    print(f"Hasta: {FECHA_FIN.strftime('%d/%m/%Y')}")
    print(f"{'='*80}\n")
    
    if not os.path.exists(NOR_PATH):
        print(f"Error: {NOR_PATH} no existe")
        return
    
    escenas = [d for d in os.listdir(NOR_PATH) 
               if os.path.isdir(os.path.join(NOR_PATH, d))]
    
    print(f"Encontradas {len(escenas)} escenas en nor\n")
    
    # Filtrar y preparar
    escenas_filtradas = []
    for escena in escenas:
        fecha = extraer_fecha(escena)
        if fecha and FECHA_INICIO <= fecha <= FECHA_FIN:
            ruta_nor = os.path.join(NOR_PATH, escena)
            escenas_filtradas.append((escena, ruta_nor))
    
    # Ordenar por nombre (fecha implícita)
    escenas_filtradas.sort(key=lambda x: x[0])
    
    print(f"{len(escenas_filtradas)} escenas del ciclo 2024/25\n")
    
    if not escenas_filtradas:
        print("No hay escenas para procesar")
        return
    
    #respuesta = input(f"Regenerar productos para {len(escenas_filtradas)} escenas con {NUM_WORKERS} workers? (s/n): ")
    #if respuesta.lower() != 's':
        #print("Procesamiento cancelado")
        #return
    print(f"Regenerando productos para {len(escenas_filtradas)} escenas con {NUM_WORKERS} workers...")
    print("\nIniciando procesamiento paralelo...\n")
    
    # Procesar en paralelo
    with Pool(processes=NUM_WORKERS) as pool:
        resultados = pool.map(procesar_escena, escenas_filtradas)
    
    # Resumen
    exitosas = sum(1 for _, exito, _ in resultados if exito)
    fallidas = sum(1 for _, exito, _ in resultados if not exito)
    
    print(f"\n{'='*80}")
    print(f"RESUMEN FINAL")
    print(f"{'='*80}")
    print(f"Exitosas: {exitosas}")
    print(f"Fallidas: {fallidas}")
    print(f"Total: {len(escenas_filtradas)}")
    
    if fallidas > 0:
        print(f"\nEscenas con errores:")
        for escena, exito, error in resultados:
            if not exito:
                print(f"  - {escena}: {error}")
    
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
