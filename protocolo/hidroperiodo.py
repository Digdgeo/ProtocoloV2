import os
import rasterio
import numpy as np
from datetime import date



def get_escenas_values(path):

    """Calcula los valores en días para cada escena dentro de un ciclo hidrológico.

    Este método genera un diccionario con las escenas como claves y los días asignados
    como valores. Estos valores se calculan basados en el inicio de un ciclo hidrológico
    (1 de septiembre).

    Args:
        path (str): Ruta al directorio que contiene las escenas (archivos .tif).

    Returns:
        dict: Un diccionario que asigna un valor en días a cada escena.
    """    
    
    escenas = [i[:8] for i in os.listdir(path) if i.endswith('.tif')]
    years = set([i[:4] for i in escenas])
    d0 = date(int(min(years)), 9, 1)
    #print(years)
    
    ndays = []
    nmeds = []
    pcortes = []
    values = []
    
    for i in sorted(escenas):
        escenaF = [int(i[:4]), int(i[4:6]), int(i[6:8])]
        d1 = date(escenaF[0], escenaF[1], escenaF[2])
        delta = d1 - d0
        ndays.append(delta.days)
    
    for n in range(len(ndays)-1):    
        p = (ndays[n+1] - ndays[n])/2
        nmeds.append(p)

    for n, e in enumerate(nmeds):
        pcortes.append(e + ndays[n])

    values.append(pcortes[0])
    for n in range(len(pcortes)-1):
        p = (pcortes[n+1] - pcortes[n])
        values.append(p)
    values.append(365-pcortes[-1])

    escenas_valor = dict(zip(sorted(escenas), values))
    
    return escenas_valor


def get_hydroperiod(path, values):

    """Genera los productos intermedios de inundación, sequía y días válidos para cada escena.

    Este método toma los archivos .tif de escenas y genera tres productos intermedios:
    - flood_rec: Inundación (días de inundación)
    - dry_rec: Sequía (días secos)
    - valid_rec: Días válidos (suma de días de inundación y sequía)

    Args:
        path (str): Ruta al directorio que contiene las escenas (archivos .tif).
        values (dict): Diccionario de valores en días para cada escena, generado por `get_escenas_values`.
    """
    
    escenas = [i for i in os.listdir(path) if i.endswith('.tif')]
    outpath = os.path.join(path, 'output')
    os.makedirs(outpath, exist_ok=True)
    
    for i in sorted(escenas):
        
        rs = os.path.join(path, i)
        #outpus
        out_flood = os.path.join(outpath,i[:-4] + '_flood_rec.tif')
        out_dry = os.path.join(outpath,i[:-4] + '_dry_rec.tif')
        out_valid = os.path.join(outpath,i[:-4] + '_valid_rec.tif')
        
        with rasterio.open(rs) as src:
            RS = src.read()
            #Inudadas
            RS_FLOOD = np.where((RS == 1), get_escenas_values(path)[i[:8]], 0)
            print(i, RS_FLOOD.mean())
            #Secas
            RS_DRY = np.where((RS == 0), get_escenas_values(path)[i[:8]], 0)
            #RS_DRY = np.where((RS_DRY == 1) | (RS_DRY == 255), 0, RS_DRY)
            #Validas
            RS_VALID = RS_DRY + RS_FLOOD
            
            profile = src.meta
            profile.update(dtype=rasterio.float32)

            with rasterio.open(out_flood, 'w', **profile) as dst:
                dst.write(RS_FLOOD.astype(rasterio.float32))
            with rasterio.open(out_dry, 'w', **profile) as dst:
                dst.write(RS_DRY.astype(rasterio.float32))
            with rasterio.open(out_valid, 'w', **profile) as dst:
                dst.write(RS_VALID.astype(rasterio.float32))
                
                
def get_products(path):

    """Genera el hidroperiodo y los días válidos acumulados de todas las escenas.

    Este método combina todas las escenas procesadas en `get_hydroperiod` para calcular
    dos productos finales:
    - hydroperiod.tif: Muestra el número total de días de inundación a lo largo de todas las escenas.
    - valid_days.tif: Muestra el número total de días válidos (inundación + sequía) a lo largo de todas las escenas.

    Args:
        path (str): Ruta al directorio que contiene los archivos intermedios (_flood_rec.tif, _valid_rec.tif).
    """    
    
    floods = [os.path.join(path, i) for i in os.listdir(path) if i.endswith('_flood_rec.tif')]
    #print(floods)
    #dry = [os.path.join(path, i) for i in os.listdir(path) if i.endswith('_dry_rec.tif')]
    valids = [os.path.join(path, i) for i in os.listdir(path) if i.endswith('_valid_rec.tif')]

     # Ordenar y extraer el ciclo
    try:
        # Extraer solo el nombre del archivo
        first_file_name = os.path.basename(sorted(floods)[0])
        c1 = first_file_name[:4]
        c2 = int(c1) + 1
        ciclo = f'_{c1}_{c2}'
    except Exception as e:
        print(f"Error al extraer el ciclo: {e}")
        return 
            
    out_flood = os.path.join(os.path.split(path)[0], f'hydroperiod{ciclo}.tif')
    out_valid = os.path.join(os.path.split(path)[0], f'valid_days{ciclo}.tif')
    
    #Generate the hydroperiod and valid days bands
    shape = rasterio.open(floods[0]).read().shape
    
    #Generate zeros arrays to fill with the correct values
    zerosf = np.zeros(shape)
    zerosv = np.zeros(shape)
    first = np.zeros(shape)
    last = np.zeros(shape)
    
    for i in floods:
        arrf = rasterio.open(i).read()
        zerosf += arrf
        #first np.where((arrf != 0) & (arrf != 255), 
    for i in valids:
        arrv = rasterio.open(i).read()
        zerosv += arrv
    
    meta = rasterio.open(floods[0]).meta
    #meta.update(count=len(floods))
    
    with rasterio.open(out_flood, 'w', **meta) as dst:
        dst.write(zerosf.astype(rasterio.float32))
    with rasterio.open(out_valid, 'w', **meta) as dst:
        dst.write(zerosv.astype(rasterio.float32))


def get_normalized_365(path):

    """Genera el hidroperiodo normalizado a 365 días.

    Utiliza los productos hydroperiod.tif y validdays.tif para generar el hidroperiodo normalizado
    en el que el 100% equivale a 365 días de inundación.

    Args:
        path (str): Ruta al directorio que contiene los archivos intermedios hydroperiod.tif y valid_days.tif.
    """    

    for i in os.listdir(path):
        if i.startswith('hydroperiod'):            
            hyd_cycle = os.path.join(path, i)
            # Capturamos el ciclo
            ciclo = i.split('_')[1] + '_' + i.split('_')[-1][:-4]
        elif i.startswith('valid_days'):
            val_days = os.path.join(path, i)
        else: continue

    with rasterio.open(hyd_cycle) as hyd_src:
        meta = hyd_src.meta
        HYD = hyd_src.read()

        with rasterio.open(val_days) as val_src:
            VAL = val_src.read()
        
            #Inudadas
            NOR_HYD = np.true_divide(HYD, VAL) * 365
            NOR_HYD_CLIP = np.clip(NOR_HYD, 0, 365)
        
            out_nor_hyd = os.path.join(path, f'hydroperiod_nor_{ciclo}.tif')
        
            with rasterio.open(out_nor_hyd, 'w', **meta) as dst:
                dst.write(NOR_HYD_CLIP)