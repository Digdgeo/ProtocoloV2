import os
import glob
from datetime import datetime
import requests
import xarray as xr
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import rasterio
from rasterio.mask import mask
from shapely.geometry import LineString
import cv2
import fiona

class Coast:
    
    def __init__(self, pro_escena_path, mtl_dict=None, nombre_mask=None, zona='Bonanza_Bon2_3333'):

        """
        Initializes the Coast class for extracting coastal lines and embryonic dunes from Landsat scenes.

        Args:
            pro_escena_path (str): Path to the processed scene directory.
            mtl_dict (dict, optional): Metadata dictionary from the Landsat MTL file containing scene information (default: None).
            nombre_mask (str, optional): Filename of the flood mask to use. If None, the function searches for a '_flood.tif' file within the scene directory.
            zona (str, optional): PortusCopia tide gauge zone code (default: 'Bonanza_Bon2_3333').

        Sets:
            fecha (datetime.date): Acquisition date extracted from the scene ID.
            hora_local (str): Default Landsat local time of acquisition in HH:MM format (10:15).
            mascara_agua (str): Absolute path to the flood mask file.
            costa_extent (str): Absolute path to the coastline clipping shapefile.
            ndvi_escena (str, optional): Absolute path to the NDVI raster file, if found in the scene directory.
        """
        
        self.escena_path = pro_escena_path
        self.nombre_escena = os.path.basename(pro_escena_path)

        self.pro = os.path.dirname(pro_escena_path)
        self.base = os.path.dirname(self.pro)

        self.mtl = mtl_dict
        self.zona = zona
        self.nc_path = None
        self.slev_value = 0.0
        self.linea_costa = None

        fecha_str = self.nombre_escena[:8]
        self.fecha = datetime.strptime(fecha_str, "%Y%m%d").date()
        self.hora_local = "10:15"

        if nombre_mask:
            self.mascara_agua = os.path.join(pro_escena_path, nombre_mask)
        else:
            posibles = glob.glob(os.path.join(pro_escena_path, '*_flood.tif'))
            self.mascara_agua = posibles[0] if posibles else None

        if self.mascara_agua:
            print(f"Máscara de agua encontrada: {self.mascara_agua}")
        else:
            print("⚠️ No se encontró máscara de agua")

        self.costa_extent = os.path.join(self.base, 'data', 'costa_extent.shp')
        self.pro_escena = pro_escena_path
        self.ndvi = glob.glob(os.path.join(pro_escena_path, '*_ndvi_.tif'))
        self.ndvi_escena = self.ndvi[0] if self.ndvi else None

    def descargar_nivel_mar(self):

        """Downloads the sea level data for the given scene from the PortusCopia API.

        This method fetches the sea level data for the scene based on its
        acquisition date and stores the file locally. If the scene's year
        is before 1993, it skips the download and assigns a default sea
        level value of 0.0.

        Args:
            scene (str): The identifier or name of the scene for which to
                download sea level data.

        Raises:
            Exception: If the download fails or the data is not available
                for the scene's year.
        """

        if self.fecha.year < 1993:
            print("⚠️ No hay datos de marea disponibles para fechas anteriores a 1993. Se asignará valor 0.")
            self.nc_path = None
            self.slev_value = 0.0
            return

        fecha_str = self.fecha.strftime("%Y%m%d")
        mes_str = self.fecha.strftime("%m")
        url = f"http://opendap.puertos.es/thredds/fileServer/tidegauge_bon2/{self.fecha.year}/{mes_str}/MIR2Z_{self.zona}_{fecha_str}.nc4"

        os.makedirs(os.path.join(self.base, 'coast'), exist_ok=True)
        self.nc_path = os.path.join(self.base, 'coast', f"{fecha_str}.nc4")

        if not os.path.exists(self.nc_path):
            print(f"Descargando {fecha_str} desde {url}")
            r = requests.get(url, timeout=60)
            if r.status_code == 200:
                with open(self.nc_path, 'wb') as f:
                    f.write(r.content)
            else:
                raise Exception(f"No se pudo descargar el archivo: {url}")
        else:
            print(f"Archivo ya descargado: {self.nc_path}")

    def extraer_marea_en_hora(self, media_anual=5.4387):

        """
        Extracts the sea level at a specific time (based on the local time of the scene)
        from the downloaded sea level data.

        This method retrieves the sea level data for the scene's acquisition time,
        adjusts it based on the annual mean sea level, and stores the result.

        Args:
            media_anual (float, optional): The annual mean sea level in meters to
                adjust the retrieved value. Defaults to 5.4387.

        Raises:
            ValueError: If the sea level data is not available for the given time.
        """
        
        if not self.nc_path or not os.path.exists(self.nc_path):
            print("⚠️ No hay archivo de marea disponible para esta escena. Se mantiene valor por defecto: 0.0")
            return

        hora_utc = datetime.combine(self.fecha, datetime.strptime(self.hora_local, "%H:%M").time())

        ds = xr.open_dataset(self.nc_path)
        slev = ds["SLEV"].sel(DEPTH=0)
        attrs = ds["SLEV"].attrs

        slev_interp = slev.sel(TIME=hora_utc, method="nearest").values.item()

        scale_factor = 1.0
        if 'scale_factor' in attrs:
            scale_factor = float(attrs['scale_factor'])
        elif attrs.get("units", "").lower() in ['cm', 'centimeters']:
            scale_factor = 0.01

        valor_original = float(slev_interp) * scale_factor
        valor_centrado = valor_original - media_anual

        self.slev_value = round(valor_centrado, 3)
        print(f"Nivel del mar a las {hora_utc} UTC (ajustado a media 2024): {self.slev_value} m")
        ds.close()

    def obtener_linea_costa(self, mask_path):

        """
        Extracts the coastline from a given water mask using contour detection.

        This method detects the coastline by finding contours in the provided water mask
        (binary mask where water is represented by 1) and then simplifies the coastline
        using the Shapely library. The result is clipped using the class attribute
        `self.costa_extent` shapefile and stored as a shapefile with the scene ID.

        Args:
            mask_path (str): The path to the water mask file (_flood.tif) used to detect the coastline.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the coastline geometry (as LineString)
                and a 'sea_level' column with the corresponding sea level height.

        Raises:
            ValueError: If no coastline is detected after processing the contours.
        """
        
        with rasterio.open(mask_path) as src:
            water_mask = src.read(1)
            transform = src.transform
            crs = src.crs
            nodata = src.nodata
    
        water_mask_bin = (water_mask == 1).astype(np.uint8) * 255
    
        border_width = 15
        height, width = water_mask_bin.shape
        water_mask_bin[:border_width, :] = 0
        water_mask_bin[-border_width:, :] = 0
        water_mask_bin[:, :border_width] = 0
        water_mask_bin[:, -border_width:] = 0
    
        contours, _ = cv2.findContours(water_mask_bin, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
        coast_contours = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            x, y, w, h = cv2.boundingRect(cnt)
            aspect_ratio = max(w, h) / min(w, h) if min(w, h) > 0 else 0
            if area > 1000 and aspect_ratio < 5:
                coast_contours.append(cnt)
    
        if not coast_contours:
            raise ValueError("No se detectó la línea de costa. Reduce 'border_width' o ajusta los filtros.")
    
        coastline_contour = max(coast_contours, key=cv2.contourArea)
        coastline_points = [transform * (point[0][0], point[0][1]) for point in coastline_contour]
        coastline_line = LineString(coastline_points)
    
        # Aplicar suavizado
        tolerancia = 0.35
        linea_suavizada = coastline_line.simplify(tolerancia, preserve_topology=True)
    
        # Crear GeoDataFrame
        gdf = gpd.GeoDataFrame(geometry=[linea_suavizada], crs=crs)
    
        # Clip con costa_extent
        costa_recorte = gpd.read_file(self.costa_extent)
        if costa_recorte.crs != gdf.crs:
            costa_recorte = costa_recorte.to_crs(gdf.crs)
        gdf = gpd.overlay(gdf, costa_recorte, how='intersection')
    
        # Añadir campo de altura
        gdf["altura_marea"] = self.slev_value
        self.linea_costa = gdf
    
        # Guardar shapefile con nombre basado en el ID de escena
        scene_id = os.path.basename(mask_path).replace('_flood.tif', '')
        nombre_salida = f"{scene_id}_coastline.shp"
        salida = os.path.join(self.pro_escena, nombre_salida)
        gdf.to_file(salida)
        print(f"Línea de costa guardada en {salida}.")
        return gdf


    def obtener_duna_embrionaria(self, ndvi_path):

        """
        Extracts the embryonic dune line from an NDVI raster.

        This method identifies the embryonic dune by creating a binary mask of vegetation
        (NDVI > 0.15, a threshold indicating potential vegetation) and then generating
        contours from the NDVI mask. It also buffers the extracted coastline (from self.costa)
        by approximately 300 meters to restrict the potential dune line to areas near the coast.
        The resulting embryonic dune line(s) are stored as LineString geometries in a GeoDataFrame.

        Args:
            ndvi_path (str): The path to the NDVI raster file used to detect the embryonic dune.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the embryonic dune line(s) geometry (as LineString)
                and a 'sea_level' column with the corresponding sea level height.

        Raises:
            ValueError: If no contours are found or if the identified dune area is too small
                to be considered a valid embryonic dune.
        """ 
        
        # 1. Leer el raster NDVI
        with rasterio.open(ndvi_path) as src:
            ndvi = src.read(1)
            transform = src.transform
            crs = src.crs
    
        # 2. Crear máscara binaria de vegetación embrionaria (NDVI > 0.15)
        ndvi_mask = (ndvi > 0.15).astype(np.uint8) * 255
    
        # 3. Crear buffer desde línea de costa hacia el interior (~300m)
        buffer_metros = 300
        costa_buffer = self.linea_costa.copy()
        costa_buffer = costa_buffer.to_crs(epsg=32630)  # asegúrate que sea en metros
        buffered = costa_buffer.buffer(buffer_metros)
        buffered = gpd.GeoDataFrame(geometry=buffered, crs=costa_buffer.crs)
    
        # Volver al CRS del NDVI si es diferente
        if buffered.crs != crs:
            buffered = buffered.to_crs(crs)
    
        # 4. Recorte por buffer usando geometry_mask
        shapes = [(geom, 1) for geom in buffered.geometry]
        mask_shape = ndvi.shape
        mask_buffer = geometry_mask(shapes, transform=transform, invert=True, out_shape=mask_shape)
        ndvi_mask = ndvi_mask * mask_buffer.astype(np.uint8)
    
        # 5. Buscar múltiples contornos
        contours, _ = cv2.findContours(ndvi_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
        if not contours:
            raise ValueError("No se encontraron contornos de duna embrionaria.")
    
        tolerancia = 0.35
        min_area = 500
    
        lineas_duna = []
        for cnt in contours:
            area = cv2.contourArea(cnt)
            if area > min_area:
                puntos = [transform * (pt[0][0], pt[0][1]) for pt in cnt]
                linea = LineString(puntos)
                linea_suave = linea.simplify(tolerancia, preserve_topology=True)
                lineas_duna.append(linea_suave)
    
        if not lineas_duna:
            raise ValueError("No se generó ninguna línea con área suficiente.")
    
        # 6. Crear GeoDataFrame con todas las líneas
        gdf = gpd.GeoDataFrame(geometry=lineas_duna, crs=crs)
        gdf["altura_marea"] = self.slev_value
    
        # 7. Guardar
        scene_id = os.path.basename(ndvi_path).replace('_ndvi.tif', '')
        salida = os.path.join(self.pro_escena, f"{scene_id}_duna_embrionaria.shp")
        gdf.to_file(salida)
    
        print(f"[INFO] {len(gdf)} línea(s) de duna embrionaria guardadas en: {salida}")
        self.linea_duna = gdf
        return gdf

    
    def graficar_nivel_mar_diario(self, save_path=None, media_anual=5.4387):

        """
        Plots the daily sea level and saves or displays the graph.

        This method generates a plot of the daily sea level data (in meters), adjusting
        it with the provided annual mean value. It also highlights the specific sea
        level at the time of the Landsat scene acquisition. The plot can be saved
        to a file or displayed interactively.

        Args:
            save_path (str, optional): The path where the plot will be saved. If None, the
                plot will be displayed instead.
            media_anual (float, optional): The annual mean sea level value in meters used to
                adjust the daily data (default is 5.4387, corresponding to the 2024 mean).

        Returns:
            None: The method either displays or saves the graph and does not return a value.
        """

        if not self.nc_path or not os.path.exists(self.nc_path):
            print("⚠️ No hay archivo de marea disponible para esta escena.")
            return

        hora_utc = datetime.combine(self.fecha, datetime.strptime(self.hora_local, "%H:%M").time())

        ds = xr.open_dataset(self.nc_path)
        slev = ds["SLEV"].sel(DEPTH=0).to_dataframe().reset_index()
        ds.close()

        slev["slev_ajustada"] = slev["SLEV"] - media_anual

        plt.figure(figsize=(12, 4))
        plt.plot(slev["TIME"], slev["slev_ajustada"], label="Nivel del mar (ajustado)")
        plt.axvline(x=hora_utc, color='red', linestyle='--', label="Hora Landsat")
        if self.slev_value is not None:
            plt.axhline(y=self.slev_value, color='orange', linestyle=':', label=f"Marea escena: {self.slev_value} m")
        plt.title(f"Nivel del mar diario - {self.fecha}")
        plt.xlabel("Hora (UTC)")
        plt.ylabel("Nivel (m, respecto a media 2024)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path)
            print(f"Gráfico guardado en: {save_path}")
        else:
            plt.show()

    def run(self):

        """
        Executes the full process for generating the coastline and embryonic dune lines.

        This method orchestrates the entire process of:
            1. Downloading tide data.
            2. Extracting the sea level at the time of the Landsat scene.
            3. Generating the coastline based on the provided water mask.
            4. Optionally generating the embryonic dune line based on the available NDVI image.
            5. Plotting the daily sea level.

        The method expects the `self.pro_escena_path` and relevant attributes (e.g., `self.fecha`)
        to be initialized. The generated coastline and (optionally) embryonic dune line
        will be stored as shapefiles. The sea level plot will be saved or displayed.

        Any errors encountered during each step will be printed to the console.

        Returns:
            None: The method performs the actions but does not return a value.
        """
        
        print("🌊 Iniciando proceso completo de línea de costa y duna...")
    
        try:
            print("⏬ Descargando nivel del mar...")
            self.descargar_nivel_mar()
            print("✅ Nivel del mar descargado.")
        except Exception as e:
            print(f"❌ Error al descargar nivel del mar: {e}")
            return
    
        try:
            print("⏱️  Extrayendo marea en hora definida...")
            self.extraer_marea_en_hora()
            print(f"✅ Marea extraída: {self.slev_value:.2f} m")
        except Exception as e:
            print(f"❌ Error al extraer la marea: {e}")
            return
    
        try:
            print("🟦 Generando línea de costa...")
            self.obtener_linea_costa(self.mascara_agua)
            print("✅ Línea de costa generada.")
        except Exception as e:
            print(f"❌ Error al generar la línea de costa: {e}")
            return
    
        if self.ndvi_escena:
            try:
                print("🟩 Generando línea de duna embrionaria...")
                self.obtener_duna_embrionaria(self.ndvi_escena)
                print("✅ Línea de duna embrionaria generada.")
            except Exception as e:
                print(f"⚠️  No se pudo generar la línea de duna embrionaria: {e}")
        else:
            print("⚠️  NDVI no disponible. No se generó línea de duna embrionaria.")

        self.graficar_nivel_mar_diario()
    
        print("🏁 Proceso completo finalizado.")
