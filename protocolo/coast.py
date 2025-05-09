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

    """
    Generate coastal products (coastline and embryonic dune) from a processed Landsat scene.

    This class retrieves sea level data from the PortusCopia API and uses the flood mask 
    and NDVI raster to extract the coastline and dune front as shapefiles. It also provides 
    visual diagnostics of sea level for the acquisition date.

    See Also
    --------
    __init__ : Initializes paths, metadata, and input masks needed for processing.
    run : Executes the full sequence for downloading tide data, extracting features, 
          and saving outputs.
    """
    
    def __init__(self, pro_escena_path, mtl_dict=None, nombre_mask=None, zona='Bonanza_Bon2_3333'):

        """
        Initialize the Coast object with scene paths, metadata, and flood mask.

        This method sets up internal attributes based on the processed Landsat scene directory.
        It locates the flood mask and NDVI file (if available), extracts the acquisition date 
        from the scene name, and sets default parameters for tide zone and local acquisition time.

        Parameters
        ----------
        pro_escena_path : str
            Path to the processed scene directory (`pro/escena_id`).

        mtl_dict : dict, optional
            Metadata dictionary extracted from the MTL file of the Landsat scene.

        nombre_mask : str, optional
            Filename of the flood mask. If None, it searches for a file matching `*_flood.tif` in the scene folder.

        zona : str, optional
            PortusCopia tide gauge zone code used to construct the download URL (default is 'Bonanza_Bon2_3333').

        Notes
        -----
        - If no flood mask is found, a warning is printed.
        - The NDVI file is optional and will be used only if available.
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

        """
        Download sea level data for the scene from the PortusCopia API.

        This method fetches the sea level data (`.nc4` file) for the acquisition date of the scene 
        using the configured tide gauge zone. If the scene is dated before 1993, the download is skipped 
        and the sea level is set to 0.0 by default.

        The file is saved in the `coast` folder within the base project directory.

        Raises
        ------
        Exception
            If the request fails or the file cannot be downloaded.
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
        Extract the sea level at the Landsat scene acquisition time.

        This method reads the downloaded sea level dataset (`.nc4`) and retrieves the sea level 
        value closest to the scene's local acquisition time (converted to UTC). The value is 
        adjusted by subtracting the annual mean sea level.

        The final sea level is stored in `self.slev_value`.

        Parameters
        ----------
        media_anual : float, optional
            Annual mean sea level (in meters) to center the value (default is 5.4387, typical for 2024).

        Raises
        ------
        ValueError
            If the sea level value cannot be retrieved from the file.
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
        Extract the coastline from a binary water mask using contour detection.

        This method processes a flood mask raster (where water is coded as 1) to detect 
        the coastline. It applies contour detection and filtering based on area and shape, 
        simplifies the resulting geometry, clips it using a predefined shapefile, and 
        stores the output as a shapefile with sea level metadata.

        Parameters
        ----------
        mask_path : str
            Path to the water mask raster file (e.g., `*_flood.tif`).

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame containing the coastline geometry (as LineString) and the sea level.

        Raises
        ------
        ValueError
            If no valid coastline is detected after contour filtering.
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
        Extract the embryonic dune line from an NDVI raster.

        This method identifies vegetated areas (NDVI > 0.15) near the coastline, 
        using a 300-meter buffer from the previously extracted coastline. It detects 
        contours in the vegetation mask, filters them by area, simplifies the geometry, 
        and stores the result as a shapefile with sea level information.

        Parameters
        ----------
        ndvi_path : str
            Path to the NDVI raster file corresponding to the scene.

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame with one or more LineString geometries representing the embryonic dune lines.

        Raises
        ------
        ValueError
            If no valid dune contours are detected or if all are too small.

        Notes
        -----
        The detection of embryonic dunes using NDVI is limited by the 30-meter resolution 
        of Landsat imagery. This method is experimental and results may be noisy or incomplete.
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
        Plot daily sea level time series and highlight the Landsat acquisition time.

        This method visualizes sea level data for the date of the Landsat scene,
        adjusted by the annual mean sea level. It marks the acquisition time with a 
        vertical line and optionally saves the figure to disk.

        Parameters
        ----------
        save_path : str, optional
            Path to save the plot. If None, the plot is displayed interactively.

        media_anual : float, optional
            Annual mean sea level (in meters) used to center the values 
            (default is 5.4387, typical for 2024).

        Returns
        -------
        None

        Notes
        -----
        This plot is useful for interpreting the relative sea level during the Landsat
        acquisition, but the Landsat scene does not capture sea level directly.
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
        Execute the full workflow to extract coastal and dune features from a Landsat scene.

        This method orchestrates the complete process of coastal feature generation:
        
        1. Download sea level data from PortusCopia.
        2. Extract the sea level value at the Landsat scene acquisition time.
        3. Generate the coastline from the flood mask.
        4. Optionally generate the embryonic dune line from the NDVI raster (if available).
        5. Plot the daily sea level time series.

        Any exceptions encountered during the steps are printed to the console.

        Returns
        -------
        None

        Notes
        -----
        This workflow is tailored for Landsat-derived flood masks and NDVI products, 
        but detection of fine features like embryonic dunes may be limited due to 
        the 30-meter spatial resolution of Landsat imagery.
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
