import os
import rasterio
import numpy as np
from datetime import date



def get_escenas_values(path):

    '''Con este script, podemos calcular el valor en días que le corresponde a una escena dentro de un ciclo 
    hidrológico. Con suerte, será la base para el cálculo del hidroperiodo'''
    
    
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
    
    
    floods = [os.path.join(path, i) for i in os.listdir(path) if i.endswith('_flood_rec.tif')]
    print(floods)
    #dry = [os.path.join(path, i) for i in os.listdir(path) if i.endswith('_dry_rec.tif')]
    valids = [os.path.join(path, i) for i in os.listdir(path) if i.endswith('_valid_rec.tif')]
                
            
    out_flood = os.path.join(os.path.split(path)[0], 'hydroperiod.tif')
    out_valid = os.path.join(os.path.split(path)[0], 'valid_days.tif')
    
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