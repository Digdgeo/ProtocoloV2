import os
import rasterio
import numpy as np
from datetime import date

"""
Hydroperiod processing module for binarized Landsat scenes.

This module includes standalone functions to calculate:
- Relative day weights for each scene in a hydrological year.
- Intermediate rasters for flood, dry, and valid days.
- Final hydroperiod and valid days rasters.
- A normalized hydroperiod scaled to 365 days.

These functions are designed to work with binary raster masks derived from
Landsat data, where flooded pixels are coded as 1 and dry pixels as 0.

Notes
-----
In an upcoming refactor, this module will be reorganized into a `Hydroperiod` class 
that will group these functions along with related utilities currently located in `utils.py`. 
The goal is to simplify integration with the Landsat product pipeline and improve modularity.
"""


def get_escenas_values(path):

    """
    Assign relative day values to each scene in a hydrological cycle.

    Calculates the number of days since the beginning of the hydrological year
    (starting September 1st) for each `.tif` file in the given folder, and assigns 
    weights based on the time intervals between scenes.

    Parameters
    ----------
    path : str
        Path to the directory containing the scene `.tif` files.

    Returns
    -------
    dict
        Dictionary mapping each scene (by date) to its relative day value.
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

    """
    Generate intermediate hydroperiod products: flood, dry, and valid day rasters.

    For each scene `.tif` file in the folder, creates three rasters representing:
    - Flood days: pixels with value 1 multiplied by the scene's day weight.
    - Dry days: pixels with value 0 multiplied by the same day weight.
    - Valid days: sum of flood and dry values.

    Parameters
    ----------
    path : str
        Path to the directory containing the binary scene `.tif` files.

    values : dict
        Dictionary of day weights for each scene, produced by `get_escenas_values`.

    Returns
    -------
    None
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

    """
    Aggregate all intermediate rasters into final hydroperiod and valid days products.

    Combines all `_flood_rec.tif` and `_valid_rec.tif` rasters to compute:
    - `hydroperiod_XXXX_YYYY.tif`: total flood days across the hydrological cycle.
    - `valid_days_XXXX_YYYY.tif`: total valid days (flood + dry).

    Parameters
    ----------
    path : str
        Path to the folder containing intermediate `_flood_rec.tif` and `_valid_rec.tif` rasters.

    Returns
    -------
    None
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

    """
    Generate the normalized hydroperiod raster, scaled to a 365-day year.

    Uses the `hydroperiod_XXXX_YYYY.tif` and `valid_days_XXXX_YYYY.tif` rasters to calculate 
    a normalized flood duration (in days) as if all scenes covered a full year.
    The result is clipped to the range [0, 365].

    Parameters
    ----------
    path : str
        Path to the directory containing the hydroperiod and valid days rasters.

    Returns
    -------
    None
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