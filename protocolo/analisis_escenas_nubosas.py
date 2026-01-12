# ============================================================================
# SCRIPT DE ANÁLISIS DE ESCENAS NUBOSAS - ESTADÍSTICAS DETALLADAS
# ============================================================================

import os
import glob
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Configuración de MongoDB
client = MongoClient()
database = client.Satelites
db = database.Landsat

# Configuración de rutas
path_base = '/mnt/datos_last/'
nor = os.path.join(path_base, 'nor')
pro = os.path.join(path_base, 'pro')

# ============================================================================
# FUNCIONES DE ANÁLISIS
# ============================================================================

def verificar_normalizacion_completa(escena_name, ruta_nor):
    """
    Verifica el estado de normalización de una escena.
    Retorna un diccionario con detalles.
    """
    ruta_completa = os.path.join(ruta_nor, escena_name)
    
    if not os.path.exists(ruta_completa):
        return {
            'existe': False,
            'bandas_norm': 0,
            'total_tifs': 0,
            'completa': False
        }
    
    # Buscar archivos normalizados
    archivos_norm = glob.glob(os.path.join(ruta_completa, '*_grn2_*.tif'))
    archivos_total = glob.glob(os.path.join(ruta_completa, '*.tif'))
    
    return {
        'existe': True,
        'bandas_norm': len(archivos_norm),
        'total_tifs': len(archivos_total),
        'completa': len(archivos_norm) >= 4
    }


def verificar_productos_generados(escena_name, ruta_pro):
    """
    Verifica qué productos han sido generados para una escena.
    """
    ruta_escena = os.path.join(ruta_pro, escena_name, escena_name)
    
    if not os.path.exists(ruta_escena):
        return {
            'existe': False,
            'productos': [],
            'total': 0
        }
    
    productos = {
        'rgb': os.path.exists(os.path.join(ruta_escena, f"{escena_name}_rgb.png")),
        'flood': os.path.exists(os.path.join(ruta_escena, f"{escena_name}_flood.png")),
        'superficie_inundada': os.path.exists(os.path.join(ruta_escena, f"{escena_name}_superficie_inundada.csv")),
        'lagunas': os.path.exists(os.path.join(ruta_escena, f"{escena_name}_lagunas.csv")),
        'lagunas_principales': os.path.exists(os.path.join(ruta_escena, f"{escena_name}_lagunas_principales.csv")),
        'censo': os.path.exists(os.path.join(ruta_escena, f"{escena_name}_censo_aereo.csv")),
    }
    
    return {
        'existe': True,
        'productos': [k for k, v in productos.items() if v],
        'total': sum(productos.values())
    }


def analizar_escenas_nubosas(umbral_nubes=20):
    """
    Genera un análisis completo de las escenas con nubes.
    """
    print("\n" + "="*80)
    print(f"ANÁLISIS DETALLADO DE ESCENAS CON >{umbral_nubes}% NUBES")
    print("="*80 + "\n")
    
    # Consultar MongoDB
    query = {'Clouds.cloud_RBIOS': {'$gt': umbral_nubes}}
    projection = {
        '_id': 1,
        'Clouds.cloud_RBIOS': 1,
        'usgs_id': 1,
        'Productos': 1
    }
    
    escenas = list(db.find(query, projection).sort('_id', 1))
    
    print(f"📊 Total de escenas en MongoDB: {len(escenas)}\n")
    
    # Analizar cada escena
    datos = []
    
    for doc in escenas:
        escena_id = doc['_id']
        cloud_value = doc.get('Clouds', {}).get('cloud_RBIOS', 0)
        anio = escena_id[:4]
        
        # Verificar normalización
        norm_info = verificar_normalizacion_completa(escena_id, nor)
        
        # Verificar productos
        prod_info = verificar_productos_generados(escena_id, pro)
        
        datos.append({
            'escena': escena_id,
            'anio': anio,
            'nubes': cloud_value,
            'normalizada': norm_info['completa'],
            'bandas_norm': norm_info['bandas_norm'],
            'productos_existe': prod_info['existe'],
            'num_productos': prod_info['total'],
            'productos': ', '.join(prod_info['productos']) if prod_info['productos'] else 'Ninguno'
        })
    
    # Crear DataFrame
    df = pd.DataFrame(datos)
    
    # ESTADÍSTICAS GENERALES
    print("\n" + "="*80)
    print("ESTADÍSTICAS GENERALES")
    print("="*80)
    print(f"Total escenas: {len(df)}")
    print(f"Escenas normalizadas: {df['normalizada'].sum()}")
    print(f"Escenas NO normalizadas: {(~df['normalizada']).sum()}")
    print(f"Escenas con productos: {df['productos_existe'].sum()}")
    print(f"Escenas sin productos: {(~df['productos_existe']).sum()}")
    print(f"Promedio % nubes: {df['nubes'].mean():.2f}%")
    print(f"Rango % nubes: {df['nubes'].min():.1f}% - {df['nubes'].max():.1f}%")
    
    # ESTADÍSTICAS POR AÑO
    print("\n" + "="*80)
    print("ESTADÍSTICAS POR AÑO")
    print("="*80)
    print(f"{'Año':<6} {'Total':>6} {'Norm':>6} {'NoNorm':>6} {'ConProd':>8} {'SinProd':>8} {'%Nubes':>8}")
    print("-"*80)
    
    for anio in sorted(df['anio'].unique()):
        df_anio = df[df['anio'] == anio]
        total = len(df_anio)
        norm = df_anio['normalizada'].sum()
        no_norm = total - norm
        con_prod = df_anio['productos_existe'].sum()
        sin_prod = total - con_prod
        prom_nubes = df_anio['nubes'].mean()
        
        print(f"{anio:<6} {total:>6} {norm:>6} {no_norm:>6} {con_prod:>8} {sin_prod:>8} {prom_nubes:>7.1f}%")
    
    # ESCENAS LISTAS PARA ENVIAR
    print("\n" + "="*80)
    print("ESCENAS LISTAS PARA ENVIAR (normalizadas + productos)")
    print("="*80)
    
    df_listas = df[(df['normalizada']) & (df['productos_existe'])]
    print(f"\nTotal: {len(df_listas)} escenas")
    
    if len(df_listas) > 0:
        print("\nPrimeras 10:")
        for idx, row in df_listas.head(10).iterrows():
            print(f"  {row['escena']} | {row['nubes']:.1f}% | Productos: {row['num_productos']}")
        
        if len(df_listas) > 10:
            print(f"  ... y {len(df_listas) - 10} más")
    
    # ESCENAS QUE NECESITAN PRODUCTOS
    print("\n" + "="*80)
    print("ESCENAS NORMALIZADAS SIN PRODUCTOS (necesitan ejecutar productos.py)")
    print("="*80)
    
    df_sin_prod = df[(df['normalizada']) & (~df['productos_existe'])]
    print(f"\nTotal: {len(df_sin_prod)} escenas")
    
    if len(df_sin_prod) > 0:
        print("\nPrimeras 10:")
        for idx, row in df_sin_prod.head(10).iterrows():
            print(f"  {row['escena']} | {row['nubes']:.1f}% | {row['bandas_norm']} bandas norm")
        
        if len(df_sin_prod) > 10:
            print(f"  ... y {len(df_sin_prod) - 10} más")
    
    # ESCENAS NO NORMALIZADAS
    print("\n" + "="*80)
    print("ESCENAS NO NORMALIZADAS (necesitan ejecutar protocolo)")
    print("="*80)
    
    df_no_norm = df[~df['normalizada']]
    print(f"\nTotal: {len(df_no_norm)} escenas")
    
    if len(df_no_norm) > 0:
        print("\nPrimeras 10:")
        for idx, row in df_no_norm.head(10).iterrows():
            print(f"  {row['escena']} | {row['nubes']:.1f}%")
        
        if len(df_no_norm) > 10:
            print(f"  ... y {len(df_no_norm) - 10} más")
    
    # GUARDAR RESULTADOS EN CSV
    csv_output = os.path.join(path_base, 'analisis_escenas_nubosas.csv')
    df.to_csv(csv_output, index=False)
    print(f"\n✅ Análisis completo guardado en: {csv_output}")
    
    return df


def generar_reporte_por_anio(anio, umbral_nubes=20):
    """
    Genera un reporte detallado para un año específico.
    """
    print("\n" + "="*80)
    print(f"REPORTE DETALLADO - AÑO {anio}")
    print("="*80 + "\n")
    
    # Consultar MongoDB
    query = {
        'Clouds.cloud_RBIOS': {'$gt': umbral_nubes},
        '_id': {'$regex': f'^{anio}'}
    }
    projection = {
        '_id': 1,
        'Clouds.cloud_RBIOS': 1,
        'Clouds.cloud_PN': 1,
        'usgs_id': 1,
    }
    
    escenas = list(db.find(query, projection).sort('_id', 1))
    
    if not escenas:
        print(f"No se encontraron escenas para el año {anio}")
        return
    
    print(f"Total de escenas: {len(escenas)}\n")
    print(f"{'Escena':<20} {'%RBIOS':>8} {'%PN':>8} {'Norm':>6} {'Prod':>6} {'Estado':<20}")
    print("-"*80)
    
    for doc in escenas:
        escena_id = doc['_id']
        cloud_rbios = doc.get('Clouds', {}).get('cloud_RBIOS', 0)
        cloud_pn = doc.get('Clouds', {}).get('cloud_PN', 0)
        
        norm_info = verificar_normalizacion_completa(escena_id, nor)
        prod_info = verificar_productos_generados(escena_id, pro)
        
        estado = []
        if norm_info['completa']:
            estado.append("✓Norm")
        else:
            estado.append("✗Norm")
        
        if prod_info['existe']:
            estado.append("✓Prod")
        else:
            estado.append("✗Prod")
        
        norm_symbol = "✓" if norm_info['completa'] else "✗"
        prod_symbol = "✓" if prod_info['existe'] else "✗"
        estado_text = " | ".join(estado)
        
        print(f"{escena_id:<20} {cloud_rbios:>7.1f}% {cloud_pn:>7.1f}% {norm_symbol:>6} {prod_symbol:>6} {estado_text:<20}")


def exportar_lista_para_procesamiento(umbral_nubes=20, tipo='envio'):
    """
    Exporta listas de escenas para diferentes propósitos.
    
    tipo:
        'envio' - Escenas listas para enviar (normalizadas + productos)
        'productos' - Escenas que necesitan generar productos
        'normalizacion' - Escenas que necesitan normalizar
    """
    query = {'Clouds.cloud_RBIOS': {'$gt': umbral_nubes}}
    escenas = list(db.find(query, {'_id': 1}).sort('_id', 1))
    
    lista = []
    
    for doc in escenas:
        escena_id = doc['_id']
        norm_info = verificar_normalizacion_completa(escena_id, nor)
        prod_info = verificar_productos_generados(escena_id, pro)
        
        if tipo == 'envio':
            if norm_info['completa'] and prod_info['existe']:
                lista.append(escena_id)
        elif tipo == 'productos':
            if norm_info['completa'] and not prod_info['existe']:
                lista.append(escena_id)
        elif tipo == 'normalizacion':
            if not norm_info['completa']:
                lista.append(escena_id)
    
    output_file = os.path.join(path_base, f'lista_{tipo}.txt')
    with open(output_file, 'w') as f:
        f.write('\n'.join(lista))
    
    print(f"\n✅ Lista de {len(lista)} escenas guardada en: {output_file}")
    return lista


# ============================================================================
# EJECUCIÓN
# ============================================================================

if __name__ == "__main__":
    
    print("="*80)
    print("ANÁLISIS DE ESCENAS LANDSAT CON ALTA NUBOSIDAD")
    print("="*80)
    
    # Análisis completo
    df = analizar_escenas_nubosas(umbral_nubes=20)
    
    # Exportar listas
    print("\n" + "="*80)
    print("GENERANDO LISTAS DE PROCESAMIENTO")
    print("="*80)
    
    exportar_lista_para_procesamiento(umbral_nubes=20, tipo='envio')
    exportar_lista_para_procesamiento(umbral_nubes=20, tipo='productos')
    exportar_lista_para_procesamiento(umbral_nubes=20, tipo='normalizacion')
    
    # Ejemplo de reporte por año
    # generar_reporte_por_anio(1985, umbral_nubes=20)
    
    print("\n" + "="*80)
    print("ANÁLISIS COMPLETADO")
    print("="*80 + "\n")
