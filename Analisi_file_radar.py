# Analisi_file_radar_integrato_multiprocessing_con_logging_e_Peq0.py
"""
Analizzatore Radar per Analisi Pluviometriche - Versione Completa
================================================================================
Include:
- Verifica integrità archivio completa
- Logging dettagliato con classifiche
- Supporto formato file con secondi
- Elaborazione parallela ottimizzata
- Gestione file mancanti e corrotti
- Calcolo Peq_0 (Pioggia Equivalente)
"""

import os
import re
import json
import rasterio
import rasterio.mask
import geopandas as gpd
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import warnings
from shapely.geometry import box, mapping
import gc
import tempfile
import multiprocessing as mp
from multiprocessing import Pool, Manager
from functools import partial
import psutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from pathlib import Path
from collections import defaultdict

# Ignora warning per raster senza georeferenziazione
warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)

# Percorso fisso ai dati radar
RADAR_DATA_PATH = r"M:\Doc_so\01_centro_funzionale\01_13_Radar DEWTRA\MCM_1\mergingArchive"

# Parametro fisico per Peq_0
LAMBDA_PEQ = 0.2

# -------------------------------------------------------------------------
# CONFIGURAZIONE PROCESSI
# -------------------------------------------------------------------------
def get_optimal_process_count():
    """
    Calcola il numero ottimale di processi: (core_totali // 3) - 1
    Con minimo di 1 processo
    """
    total_cores = psutil.cpu_count(logical=True)
    optimal_processes = max(1, (total_cores // 3) - 1)
    
    print(f"Sistema rilevato con {total_cores} core logici")
    print(f"Utilizzo ottimale: {optimal_processes} processi paralleli")
    
    return optimal_processes

NUM_PROCESSES = get_optimal_process_count()


# -------------------------------------------------------------------------
# FUNZIONI PER CALCOLO PEQ_0
# -------------------------------------------------------------------------
def calcola_peq0(P5, CN, lam=LAMBDA_PEQ):
    """
    Calcola Peq₀ (mm) – modello NRCS-CN generalizzato.
    
    Args:
        P5: Pioggia cumulata 5 giorni precedenti (mm)
        CN: Curve Number medio della zona
        lam: Parametro lambda (default 0.2)
    
    Returns:
        Peq₀ in mm
    """
    S = 25400 / CN - 254
    sqrt_term = S * (P5 + ((1 - lam) / 2) ** 2 * S)
    M = np.maximum(np.sqrt(sqrt_term) - ((1 + lam) / 2) * S, 0)
    return M * (1 + lam * S / (S + M))


def calcola_cn_per_zone(raster_folder):
    """
    Calcola il CN medio per ogni zona dai file raster ASC.
    
    Args:
        raster_folder: Path alla directory contenente i raster CN
    
    Returns:
        Dizionario {'IM_01': 78.4, 'IM_02': 82.1, ...}
    """
    zone_cn = {}
    raster_path = Path(raster_folder)
    
    if not raster_path.exists():
        print(f"ATTENZIONE: Directory raster CN non trovata: {raster_folder}")
        return zone_cn
    
    for asc_file in sorted(raster_path.glob("*.ASC")):
        # Estrai numero zona dal nome file
        num_match = re.search(r"(\d{1,2})$", asc_file.stem)
        if not num_match:
            print(f"  Raster {asc_file.name}: numero zona non riconosciuto, salto.")
            continue
        
        # Crea chiave zona (es: IM_01)
        zona_key = f"IM_{int(num_match.group()):02d}"
        
        try:
            with rasterio.open(asc_file) as src:
                data = src.read(1, masked=True)
                cn_medio = float(np.ma.mean(data))
                zone_cn[zona_key] = cn_medio
                print(f"  CN medio {zona_key}: {cn_medio:6.1f}")
        except Exception as e:
            print(f"  ERRORE nel leggere {asc_file.name}: {e}")
            continue
    
    return zone_cn


def calcola_cum_5d(df_24h):
    """
    Calcola la pioggia cumulata dei 5 giorni precedenti per ogni data.
    
    Args:
        df_24h: DataFrame con colonne ['Data', 'IM_01', 'IM_02', ...]
    
    Returns:
        DataFrame con stessa struttura ma con cumulate 5 giorni
    """
    cum_5d = df_24h.copy()
    zone_columns = [col for col in df_24h.columns if col != 'Data']
    
    for col in zone_columns:
        # Per ogni riga, somma i 5 giorni precedenti (escluso il giorno corrente)
        cum_5d[col] = df_24h[col].rolling(window=6, min_periods=1).sum().shift(1).fillna(0)
    
    return cum_5d


def calcola_peq0_per_tutte_zone(cum_5d, zone_cn):
    """
    Calcola Peq₀ per tutte le zone e tutte le date.
    
    Args:
        cum_5d: DataFrame con cumulate 5 giorni
        zone_cn: Dizionario con CN per zona
    
    Returns:
        DataFrame con Peq₀ calcolato
    """
    peq0_df = cum_5d.copy()
    zone_columns = [col for col in cum_5d.columns if col != 'Data']
    
    for col in zone_columns:
        if col in zone_cn:
            cn_value = zone_cn[col]
            # Applica calcola_peq0 a tutta la colonna
            peq0_df[col] = cum_5d[col].apply(
                lambda p5: calcola_peq0(p5, cn_value) if pd.notna(p5) and p5 > 0 else 0
            )
        else:
            print(f"  ATTENZIONE: CN non trovato per zona {col}, Peq₀ = 0")
            peq0_df[col] = 0
    
    return peq0_df


def somma_statistiche_con_peq0(df_statistica, df_peq0):
    """
    Somma una statistica (6h, 12h, 24h) con Peq₀.
    
    Args:
        df_statistica: DataFrame con statistica originale
        df_peq0: DataFrame con Peq₀
    
    Returns:
        DataFrame con somma statistica + Peq₀
    """
    # Merge sulle date
    merged = df_statistica.merge(df_peq0, on='Data', how='left', suffixes=('', '_peq0'))
    
    zone_columns = [col for col in df_statistica.columns if col != 'Data']
    result = merged[['Data']].copy()
    
    for col in zone_columns:
        col_peq0 = f"{col}_peq0"
        if col_peq0 in merged.columns:
            # Somma i valori, riempiendo NaN con 0
            result[col] = merged[col].fillna(0) + merged[col_peq0].fillna(0)
        else:
            result[col] = merged[col].fillna(0)
    
    return result.sort_values('Data').reset_index(drop=True)


# -------------------------------------------------------------------------
# CLASSI PER GESTIONE FILE E LOGGING
# -------------------------------------------------------------------------
class FileIntegrityReport:
    """Gestisce il report di integrità dei file radar"""
    
    def __init__(self):
        self.file_corrotti = []  # Lista di (data, ora, percorso, errore)
        self.file_mancanti = []  # Lista di (data, ora_attesa)
        self.file_validi = []    # Lista di (data, ora, percorso)
        self.giorni_analizzati = set()
    
    def aggiungi_file_corrotto(self, data, ora, percorso, errore):
        """Registra un file corrotto"""
        self.file_corrotti.append({
            'data': data.strftime('%Y-%m-%d'),
            'ora': ora.strftime('%H:%M'),
            'timestamp': ora,
            'percorso': percorso,
            'errore': str(errore)
        })
    
    def aggiungi_file_mancante(self, data, ora_attesa):
        """Registra un file mancante"""
        self.file_mancanti.append({
            'data': data.strftime('%Y-%m-%d'),
            'ora': ora_attesa.strftime('%H:%M'),
            'timestamp': ora_attesa
        })
    
    def aggiungi_file_valido(self, data, ora, percorso):
        """Registra un file valido"""
        self.file_validi.append({
            'data': data.strftime('%Y-%m-%d'),
            'ora': ora.strftime('%H:%M'),
            'timestamp': ora,
            'percorso': percorso
        })
    
    def get_summary(self):
        """Restituisce un riepilogo statistico"""
        return {
            'totale_corrotti': len(self.file_corrotti),
            'totale_mancanti': len(self.file_mancanti),
            'totale_validi': len(self.file_validi),
            'giorni_analizzati': len(self.giorni_analizzati)
        }


class LogManager:
    """Gestisce il logging thread-safe per il multiprocessing"""
    
    def __init__(self, log_directory):
        self.log_directory = Path(log_directory)
        self.log_directory.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_directory / f"analisi_radar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Setup del logger principale
        self.logger = logging.getLogger('RadarAnalyzer')
        self.logger.setLevel(logging.INFO)
        
        # File handler
        fh = logging.FileHandler(self.log_file, mode='w', encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    
    def log_info(self, message):
        """Log messaggio informativo"""
        self.logger.info(message)
    
    def log_error(self, message):
        """Log messaggio di errore"""
        self.logger.error(message)
    
    def log_warning(self, message):
        """Log messaggio di warning"""
        self.logger.warning(message)
    
    def log_integrity_report(self, report):
        """Log del report di integrità dei file"""
        summary = report.get_summary()
        
        self.log_info("=" * 100)
        self.log_info("REPORT INTEGRITA' ARCHIVIO FILE RADAR")
        self.log_info("=" * 100)
        self.log_info("")
        self.log_info(f"Giorni analizzati: {summary['giorni_analizzati']}")
        self.log_info(f"File validi: {summary['totale_validi']}")
        self.log_info(f"File corrotti/danneggiati: {summary['totale_corrotti']}")
        self.log_info(f"File mancanti: {summary['totale_mancanti']}")
        self.log_info("")
        
        # Dettaglio file corrotti
        if report.file_corrotti:
            self.log_error("-" * 100)
            self.log_error(f"FILE CORROTTI O DANNEGGIATI ({len(report.file_corrotti)} totali)")
            self.log_error("-" * 100)
            
            # Raggruppa per data
            corrotti_per_data = defaultdict(list)
            for file_info in report.file_corrotti:
                corrotti_per_data[file_info['data']].append(file_info)
            
            for data in sorted(corrotti_per_data.keys()):
                self.log_error(f"\nData: {data}")
                for file_info in sorted(corrotti_per_data[data], key=lambda x: x['ora']):
                    self.log_error(f"  - Ora: {file_info['ora']}")
                    self.log_error(f"    File: {file_info['percorso']}")
                    self.log_error(f"    Errore: {file_info['errore']}")
            self.log_error("")
        else:
            self.log_info("✓ Nessun file corrotto rilevato")
            self.log_info("")
        
        # Dettaglio file mancanti
        if report.file_mancanti:
            self.log_warning("-" * 100)
            self.log_warning(f"FILE MANCANTI ({len(report.file_mancanti)} totali)")
            self.log_warning("-" * 100)
            
            # Raggruppa per data
            mancanti_per_data = defaultdict(list)
            for file_info in report.file_mancanti:
                mancanti_per_data[file_info['data']].append(file_info)
            
            for data in sorted(mancanti_per_data.keys()):
                self.log_warning(f"\nData: {data}")
                ore_mancanti = [f['ora'] for f in sorted(mancanti_per_data[data], key=lambda x: x['ora'])]
                self.log_warning(f"  Ore mancanti: {', '.join(ore_mancanti)}")
            self.log_warning("")
        else:
            self.log_info("✓ Nessun file mancante rilevato")
            self.log_info("")
        
        self.log_info("=" * 100)
        self.log_info("")
    
    def log_finestre_analizzate(self, zona_nome, data, durata, finestre_dati):
        """
        Log dettagliato delle finestre analizzate
        finestre_dati: lista di tuple (timestamp_inizio, timestamp_fine, statistiche_dict, file_list)
        """
        self.logger.info("=" * 80)
        self.logger.info(f"ZONA: {zona_nome} | DATA: {data} | DURATA: {durata}h")
        self.logger.info("-" * 80)
        
        if not finestre_dati:
            self.logger.info("Nessuna finestra valida analizzata")
            self.logger.info("=" * 80)
            return
        
        # Ordina per media decrescente
        finestre_ordinate = sorted(finestre_dati, key=lambda x: x[2].get('media', 0), reverse=True)
        
        self.logger.info(f"Totale finestre analizzate: {len(finestre_ordinate)}")
        self.logger.info("")
        self.logger.info("CLASSIFICA PER MEDIA (ordine decrescente):")
        self.logger.info("")
        
        for idx, finestra_data in enumerate(finestre_ordinate, 1):
            inizio = finestra_data[0]
            fine = finestra_data[1]
            stats = finestra_data[2]
            file_list = finestra_data[3] if len(finestra_data) > 3 else []
            
            media = stats.get('media', 0)
            self.logger.info(f"  {idx:3d}. Finestra: {inizio.strftime('%Y-%m-%d %H:%M')} → {fine.strftime('%Y-%m-%d %H:%M')}")
            self.logger.info(f"       Media: {media:.2f} mm")
            
            # Log altre statistiche se presenti
            altre_stats = []
            if 'max' in stats:
                altre_stats.append(f"Max: {stats['max']:.2f}")
            if 'min' in stats:
                altre_stats.append(f"Min: {stats['min']:.2f}")
            if 'mediana' in stats:
                altre_stats.append(f"Mediana: {stats['mediana']:.2f}")
            if '95_perc' in stats:
                altre_stats.append(f"95°perc: {stats['95_perc']:.2f}")
            
            if altre_stats:
                self.logger.info(f"       {' | '.join(altre_stats)}")
            
            # Log dei file utilizzati
            if file_list:
                self.logger.info(f"       File utilizzati ({len(file_list)}): {', '.join(file_list)}")
            
            self.logger.info("")
        
        # Finestra massima
        max_finestra = finestre_ordinate[0]
        self.logger.info(f">>> FINESTRA CON MEDIA MASSIMA: {max_finestra[0].strftime('%Y-%m-%d %H:%M')} → {max_finestra[1].strftime('%Y-%m-%d %H:%M')}")
        self.logger.info(f">>> VALORE MEDIA MASSIMA: {max_finestra[2].get('media', 0):.2f} mm")
        self.logger.info("=" * 80)
        self.logger.info("")


# -------------------------------------------------------------------------
# FUNZIONI CORE
# -------------------------------------------------------------------------
def load_config(config_path):
    """Carica la configurazione dal file JSON."""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    config['data_inizio'] = pd.to_datetime(config['data_inizio'])
    config['data_fine'] = pd.to_datetime(config['data_fine'])
    
    # Valori di default per Peq_0
    if 'Peq_0' not in config.get('statistiche', {}):
        config['statistiche']['Peq_0'] = False
    
    if 'raster_cn_directory' not in config:
        config['raster_cn_directory'] = None
    
    return config


def estrai_timestamp(nome_file):
    """Estrae timestamp dal nome file - Supporta formato con secondi (yyyyMMddHHmmss)"""
    # Prima prova con formato completo (con secondi)
    match = re.search(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", os.path.basename(nome_file))
    if match:
        return datetime(*map(int, match.groups()))
    
    # Fallback al formato senza secondi (per compatibilità)
    match = re.search(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})", os.path.basename(nome_file))
    if match:
        return datetime(*map(int, match.groups()))
    
    return None


def verifica_integrita_archivio(data_inizio, data_fine):
    """
    Verifica l'integrità dell'archivio radar per il periodo specificato
    Controlla file mancanti e file corrotti
    
    Returns:
        FileIntegrityReport: Report completo dell'analisi
    """
    report = FileIntegrityReport()
    date_range = pd.date_range(data_inizio, data_fine)
    
    print("\n=== VERIFICA INTEGRITA' ARCHIVIO ===")
    print(f"Periodo: {data_inizio.strftime('%Y-%m-%d')} - {data_fine.strftime('%Y-%m-%d')}")
    print(f"Giorni da analizzare: {len(date_range)}")
    print("")
    
    for data_corrente in date_range:
        report.giorni_analizzati.add(data_corrente.date())
        
        # Costruisci il percorso della directory del giorno
        path_giorno = os.path.join(
            RADAR_DATA_PATH,
            data_corrente.strftime('%Y'),
            data_corrente.strftime('%m'),
            data_corrente.strftime('%d')
        )
        
        # Verifica se la directory esiste
        if not os.path.exists(path_giorno):
            # Se la directory non esiste, tutti i file del giorno sono mancanti
            for ora in range(24):
                timestamp_atteso = data_corrente.replace(hour=ora, minute=0, second=0, microsecond=0)
                report.aggiungi_file_mancante(data_corrente, timestamp_atteso)
            continue
        
        # Scansiona tutti i file .tif nella directory
        file_trovati = {}  # dizionario {timestamp: percorso_file}
        
        for nome_file in os.listdir(path_giorno):
            if nome_file.endswith('.tif'):
                file_path = os.path.join(path_giorno, nome_file)
                timestamp = estrai_timestamp(file_path)
                
                # Solo file con minuti=00 e secondi=00 (terminano con 0000.tif)
                if timestamp and timestamp.minute == 0 and timestamp.second == 0:
                    # Verifica integrità del file
                    try:
                        with rasterio.open(file_path) as src:
                            if src.count == 0:
                                report.aggiungi_file_corrotto(
                                    data_corrente,
                                    timestamp,
                                    file_path,
                                    "File senza bande raster"
                                )
                            else:
                                # Prova a leggere i dati
                                _ = src.read(1)
                                report.aggiungi_file_valido(data_corrente, timestamp, file_path)
                                file_trovati[timestamp] = file_path
                    except Exception as e:
                        report.aggiungi_file_corrotto(
                            data_corrente,
                            timestamp,
                            file_path,
                            str(e)
                        )
        
        # Verifica file mancanti (ore esatte da 00 a 23)
        for ora in range(24):
            timestamp_atteso = data_corrente.replace(hour=ora, minute=0, second=0, microsecond=0)
            if timestamp_atteso not in file_trovati:
                report.aggiungi_file_mancante(data_corrente, timestamp_atteso)
        
        # Progress ogni 10 giorni
        if len(report.giorni_analizzati) % 10 == 0:
            print(f"Verificati {len(report.giorni_analizzati)} giorni...")
    
    summary = report.get_summary()
    print(f"\n=== VERIFICA COMPLETATA ===")
    print(f"File validi: {summary['totale_validi']}")
    print(f"File corrotti: {summary['totale_corrotti']}")
    print(f"File mancanti: {summary['totale_mancanti']}")
    print("")
    
    return report


def ottieni_lista_file_giorno(data_corrente, integrity_report=None):
    """
    Restituisce la lista ordinata dei raster con minuti = 00
    Ora utilizza il report di integrità se disponibile
    """
    path = os.path.join(
        RADAR_DATA_PATH,
        data_corrente.strftime('%Y'),
        data_corrente.strftime('%m'),
        data_corrente.strftime('%d')
    )
    if not os.path.exists(path):
        print(f"Attenzione: Cartella non trovata per il giorno {data_corrente.strftime('%Y-%m-%d')}")
        return []
    
    files = []
    
    # Se abbiamo il report di integrità, usiamo quello
    if integrity_report:
        data_str = data_corrente.strftime('%Y-%m-%d')
        for file_info in integrity_report.file_validi:
            if file_info['data'] == data_str:
                files.append(file_info['percorso'])
    else:
        # Fallback al metodo originale
        for f in os.listdir(path):
            if f.endswith('.tif'):
                timestamp = estrai_timestamp(f)
                if timestamp and timestamp.minute == 0 and timestamp.second == 0:
                    file_path = os.path.join(path, f)
                    try:
                        with rasterio.open(file_path) as src:
                            if src.count > 0:
                                files.append(file_path)
                    except Exception as e:
                        print(f"File danneggiato o illeggibile: {file_path} ({str(e)})")
                        continue
    
    files_sorted = sorted(files)
    print(f"  Trovati {len(files_sorted)} raster (ore esatte) per il giorno {data_corrente.strftime('%Y-%m-%d')}")
    return files_sorted


def somma_file_tif_finestra(lista_file_con_timestamp, finestra_inizio, finestra_fine, temp_dir):
    """
    Somma i file TIF che cadono nella finestra temporale specificata
    LOGICA: Include file dall'ora di inizio fino all'ora precedente la fine
    Esempio: finestra 09:00-15:00 include file ore 09,10,11,12,13,14 (NON 15)
    """
    file_finestra = []
    timestamp_inclusi = []
    
    for file_path, timestamp in lista_file_con_timestamp:
        # Il file rappresenta la registrazione che INIZIA a timestamp
        # e dura 1 ora, quindi copre [timestamp, timestamp+1h)
        if finestra_inizio <= timestamp < finestra_fine:
            file_finestra.append(file_path)
            timestamp_inclusi.append(timestamp)
    
    if not file_finestra:
        return None, None, []
    
    # Lista dei file inclusi (solo nomi) per logging
    file_inclusi = [os.path.basename(f) for f in file_finestra]
    
    # Debug info
    print(f"    Sommando {len(file_finestra)} file per finestra {finestra_inizio.strftime('%Y-%m-%d %H:%M')}-{finestra_fine.strftime('%Y-%m-%d %H:%M')}")
    print(f"    Ore incluse: {', '.join([t.strftime('%H:%M') for t in sorted(timestamp_inclusi)])}")
    
    with rasterio.open(file_finestra[0]) as src:
        data_somma = src.read(1).astype(np.float64)
        meta = src.meta.copy()
        meta.update(dtype=np.float64)
        
        if src.crs is None:
            meta.update(crs=rasterio.CRS.from_epsg(4326))
    
    for file_path in file_finestra[1:]:
        try:
            with rasterio.open(file_path) as src_temp:
                data_temp = src_temp.read(1).astype(np.float64)
                data_somma = data_somma + data_temp
        except Exception as e:
            print(f"      ERRORE nel file {os.path.basename(file_path)}: {e}")
            continue
    
    temp_file = os.path.join(temp_dir, f"temp_somma_{finestra_inizio.strftime('%Y%m%d_%H%M')}.tif")
    with rasterio.open(temp_file, 'w', **meta) as dst:
        dst.write(data_somma, 1)
    
    return temp_file, (finestra_inizio, finestra_fine), file_inclusi


def ritaglia_raster_sommato_con_shapefile(raster_sommato_path, shapefile_path, temp_dir, finestra_info):
    """Ritaglia il raster sommato utilizzando lo shapefile"""
    if raster_sommato_path is None:
        return None, None
    
    try:
        shapefile = gpd.read_file(shapefile_path)
        shapes = [mapping(geom) for geom in shapefile.geometry]
        
        with rasterio.open(raster_sommato_path) as src:
            out_image, out_transform = rasterio.mask.mask(
                src, shapes, crop=True, nodata=-9999, all_touched=True
            )
            out_meta = src.meta.copy()
        
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "nodata": -9999
        })
        
        finestra_inizio, finestra_fine = finestra_info
        temp_file = os.path.join(temp_dir, f"temp_ritagliato_{finestra_inizio.strftime('%Y%m%d_%H%M')}.tif")
        with rasterio.open(temp_file, 'w', **out_meta) as dst:
            dst.write(out_image)
        
        return temp_file, out_image[0]
        
    except Exception as e:
        print(f"      ERRORE durante il ritaglio: {e}")
        return None, None


def calcola_statistiche_su_array_ritagliato(array_ritagliato, statistiche_config):
    """Calcola statistiche sull'array ritagliato considerando solo valori >= 0"""
    if array_ritagliato is None:
        return None
    
    try:
        mask_validi = array_ritagliato >= 0
        
        if not np.any(mask_validi):
            print("      Nessun valore valido (>= 0) dopo ritaglio.")
            return None
        
        valori_validi = array_ritagliato[mask_validi]
        
        if valori_validi.size == 0:
            return None
        
        risultato = {}
        
        if statistiche_config.get('media', False):
            risultato['media'] = np.mean(valori_validi)
        if statistiche_config.get('mediana', False):
            risultato['mediana'] = np.median(valori_validi)
        if statistiche_config.get('75_perc', False):
            risultato['75_perc'] = np.percentile(valori_validi, 75)
        if statistiche_config.get('95_perc', False):
            risultato['95_perc'] = np.percentile(valori_validi, 95)
        if statistiche_config.get('99_perc', False):
            risultato['99_perc'] = np.percentile(valori_validi, 99)
        if statistiche_config.get('max', False):
            risultato['max'] = np.max(valori_validi)
        if statistiche_config.get('min', False):
            risultato['min'] = np.min(valori_validi)
        if statistiche_config.get('std', False):
            risultato['std'] = np.std(valori_validi)
        
        return risultato
        
    except Exception as e:
        print(f"      Errore nel calcolo delle statistiche: {str(e)}")
        return None


def finestre_mobili_con_somma_e_ritaglio(file_con_timestamp, durata_ore, data_corrente, shapefile_path, statistiche_config, temp_dir):
    """
    Implementa la logica delle finestre mobili con approccio "somma prima, ritaglia dopo"
    Restituisce: (statistiche_massime, lista_tutte_finestre_per_logging)
    """
    if not file_con_timestamp:
        print("    Nessun file con timestamp valido per finestra mobile.")
        return None, []
    
    try:
        shapefile = gpd.read_file(shapefile_path)
    except Exception as e:
        print(f"    Errore nel caricamento shapefile {shapefile_path}: {e}")
        return None, []
    
    lista_finestre_log = []
    
    # CASO SPECIALE: per 24h
    if durata_ore == 24:
        file_data_corrente = []
        
        for file_path, timestamp in file_con_timestamp:
            if timestamp.date() == data_corrente.date():
                file_data_corrente.append((file_path, timestamp))
        
        print(f"    Modalità 24h per {data_corrente.strftime('%Y-%m-%d')}: usando {len(file_data_corrente)} file del giorno")
        
        if not file_data_corrente:
            print(f"    Nessun file trovato per la data {data_corrente.strftime('%Y-%m-%d')}")
            return None, []
        
        inizio_giorno = data_corrente.replace(hour=0, minute=0, second=0, microsecond=0)
        fine_giorno = inizio_giorno + timedelta(days=1)
        
        raster_sommato_path, finestra_info, file_inclusi = somma_file_tif_finestra(
            file_data_corrente, inizio_giorno, fine_giorno, temp_dir
        )
        
        if raster_sommato_path is None:
            return None, []
        
        raster_ritagliato_path, array_ritagliato = ritaglia_raster_sommato_con_shapefile(
            raster_sommato_path, shapefile_path, temp_dir, finestra_info
        )
        
        if array_ritagliato is None:
            return None, []
        
        stats = calcola_statistiche_su_array_ritagliato(array_ritagliato, statistiche_config)
        
        if stats:
            lista_finestre_log.append((inizio_giorno, fine_giorno, stats.copy(), file_inclusi))
        
        # Pulizia file temporanei
        try:
            if os.path.exists(raster_sommato_path):
                os.remove(raster_sommato_path)
            if raster_ritagliato_path and os.path.exists(raster_ritagliato_path):
                os.remove(raster_ritagliato_path)
        except:
            pass
        
        return stats, lista_finestre_log
    
    # Per altre durate, usa l'algoritmo delle finestre mobili normale
    timestamps = [timestamp for _, timestamp in file_con_timestamp]
    
    if not timestamps:
        return None, []
    
    min_time = min(timestamps)
    max_time = max(timestamps)
    
    # CORREZIONE CRITICA: Aggiungi 1 ora al max_time per includere l'ultima finestra
    # Esempio: se l'ultimo file è alle 23:00, rappresenta 23:00-24:00
    # quindi max_time_effettivo è 24:00 (00:00 del giorno dopo)
    max_time_effettivo = max_time + timedelta(hours=1)
    
    print(f"    Range temporale disponibile: {min_time.strftime('%Y-%m-%d %H:%M')} - {max_time_effettivo.strftime('%Y-%m-%d %H:%M')}")
    print(f"    Durata finestra: {durata_ore}h")
    
    risultati = []
    durata = timedelta(hours=durata_ore)
    step = timedelta(minutes=60)
    corrente = min_time
    
    # Conta finestre attese
    finestre_attese = 0
    temp_corrente = min_time
    while temp_corrente + durata <= max_time_effettivo:
        finestre_attese += 1
        temp_corrente += step
    
    print(f"    Finestre attese: {finestre_attese}")
    
    while corrente + durata <= max_time_effettivo:
        raster_sommato_path, finestra_info, file_inclusi = somma_file_tif_finestra(
            file_con_timestamp, corrente, corrente + durata, temp_dir
        )
        
        if raster_sommato_path is not None:
            raster_ritagliato_path, array_ritagliato = ritaglia_raster_sommato_con_shapefile(
                raster_sommato_path, shapefile_path, temp_dir, finestra_info
            )
            
            if array_ritagliato is not None:
                stats = calcola_statistiche_su_array_ritagliato(array_ritagliato, statistiche_config)
                if stats:
                    risultati.append((corrente, stats))
                    lista_finestre_log.append((corrente, corrente + durata, stats.copy(), file_inclusi))
            
            # Pulizia file temporanei
            try:
                if os.path.exists(raster_sommato_path):
                    os.remove(raster_sommato_path)
                if raster_ritagliato_path and os.path.exists(raster_ritagliato_path):
                    os.remove(raster_ritagliato_path)
            except:
                pass
        
        corrente += step
    
    print(f"    Finestre elaborate: {len(lista_finestre_log)}")
    
    if not risultati:
        print("    Nessuna finestra mobile valida per questa durata.")
        return None, lista_finestre_log
    
    stats_massime = max(risultati, key=lambda x: x[1].get('media', 0))[1]
    return stats_massime, lista_finestre_log


def processa_zona_giorno(args):
    """Funzione worker per il multiprocessing - processa una zona per un giorno specifico"""
    zona_path, data_corrente, durate, shapefile_directory, statistiche_config, log_queue, integrity_report = args
    
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            zona_nome = os.path.basename(zona_path).split('.')[0]
            risultati = {}
            log_data = {}
            
            print(f"  [PID {os.getpid()}] Zona: {zona_nome}, Giorno: {data_corrente.strftime('%Y-%m-%d')}")
            
            # Usa il report di integrità se disponibile
            raster_giorno = ottieni_lista_file_giorno(data_corrente, integrity_report)
            
            if not raster_giorno:
                print(f"  [PID {os.getpid()}] Nessun raster per zona {zona_nome} e giorno {data_corrente.strftime('%Y-%m-%d')}")
                return zona_nome, data_corrente, {durata: None for durata in durate}, {}
            
            file_con_timestamp = []
            for raster in raster_giorno:
                timestamp = estrai_timestamp(raster)
                if timestamp:
                    file_con_timestamp.append((raster, timestamp))
            
            if not file_con_timestamp:
                print(f"  [PID {os.getpid()}] Nessun file con timestamp valido per zona {zona_nome}")
                return zona_nome, data_corrente, {durata: None for durata in durate}, {}
            
            # Calcola per ogni durata
            for durata in durate:
                try:
                    risultato, finestre_log = finestre_mobili_con_somma_e_ritaglio(
                        file_con_timestamp, durata, data_corrente, zona_path, statistiche_config, temp_dir
                    )
                    risultati[durata] = risultato
                    log_data[durata] = finestre_log
                except Exception as e:
                    print(f"  [PID {os.getpid()}] ERRORE durata {durata}h per zona {zona_nome}: {str(e)}")
                    risultati[durata] = None
                    log_data[durata] = []
            
            # Invia dati di log alla queue
            if log_queue is not None:
                log_queue.put({
                    'zona': zona_nome,
                    'data': data_corrente.strftime('%Y-%m-%d'),
                    'finestre': log_data
                })
            
            return zona_nome, data_corrente, risultati, log_data
            
        except Exception as e:
            zona_nome = os.path.basename(zona_path).split('.')[0]
            print(f"  [PID {os.getpid()}] ERRORE CRITICO zona {zona_nome} per {data_corrente.strftime('%Y-%m-%d')}: {str(e)}")
            return zona_nome, data_corrente, {durata: None for durata in durate}, {}


def main():
    """Funzione principale con supporto multiprocessing e logging"""
    
    if len(sys.argv) != 2:
        print("Uso: python Analisi_file_radar.py config.json")
        sys.exit(1)
    
    config_path = sys.argv[1]
    config = load_config(config_path)
    
    # Inizializza il LogManager
    log_dir = config.get('log_directory', os.path.join(config['output_directory'], 'logs'))
    log_manager = LogManager(log_dir)
    
    log_manager.log_info("=" * 100)
    log_manager.log_info("INIZIO ANALISI RADAR PLUVIOMETRICA")
    log_manager.log_info("=" * 100)
    log_manager.log_info("")
    
    # FASE 1: VERIFICA INTEGRITA' ARCHIVIO
    log_manager.log_info("FASE 1: Verifica integrità archivio file radar")
    log_manager.log_info("-" * 100)
    
    integrity_report = verifica_integrita_archivio(config['data_inizio'], config['data_fine'])
    
    # Scrivi il report di integrità nel log
    log_manager.log_integrity_report(integrity_report)
    
    # FASE 2: CONFIGURAZIONE ANALISI
    log_manager.log_info("FASE 2: Configurazione analisi")
    log_manager.log_info("-" * 100)
    
    durate = config['durate_ore']
    statistiche_attive = [stat for stat, attivo in config['statistiche'].items() if attivo and stat != 'Peq_0']
    
    zone_shapefiles = sorted([f for f in os.listdir(config['shapefile_directory']) if f.endswith('.shp')])
    zone_paths = [os.path.join(config['shapefile_directory'], zona) for zona in zone_shapefiles]
    
    log_manager.log_info(f"Core disponibili: {psutil.cpu_count(logical=True)} logici, {psutil.cpu_count(logical=False)} fisici")
    log_manager.log_info(f"Processi paralleli: {NUM_PROCESSES}")
    log_manager.log_info(f"Periodo: {config['data_inizio'].strftime('%Y-%m-%d')} - {config['data_fine'].strftime('%Y-%m-%d')}")
    log_manager.log_info(f"Zone: {len(zone_shapefiles)} shapefile")
    log_manager.log_info(f"Durate: {durate} ore")
    log_manager.log_info(f"Statistiche: {statistiche_attive}")
    log_manager.log_info(f"Calcolo Peq_0: {'SI' if config['statistiche'].get('Peq_0', False) else 'NO'}")
    log_manager.log_info(f"Logica: SOMMA PRIMA, RITAGLIA DOPO (PARALLELA)")
    log_manager.log_info("")
    
    # FASE 3: ELABORAZIONE DATI
    log_manager.log_info("=" * 100)
    log_manager.log_info("FASE 3: Elaborazione dati radar")
    log_manager.log_info("=" * 100)
    log_manager.log_info("")
    
    # Struttura dati finale
    risultati_finali = {}
    for durata in durate:
        for stat in statistiche_attive:
            chiave = f"{stat}_{durata}h"
            risultati_finali[chiave] = {}
    
    # Prepara tutti i task
    date_range = pd.date_range(config['data_inizio'], config['data_fine'])
    
    # Manager per la queue di logging
    manager = Manager()
    log_queue = manager.Queue()
    
    tasks = []
    for data in date_range:
        for zona_path in zone_paths:
            task = (zona_path, data, durate, config['shapefile_directory'], 
                   config['statistiche'], log_queue, integrity_report)
            tasks.append(task)
    
    log_manager.log_info(f"Totale task da elaborare: {len(tasks)}")
    log_manager.log_info("")
    
    completed_tasks = 0
    
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        future_to_task = {executor.submit(processa_zona_giorno, task): task for task in tasks}
        
        for future in as_completed(future_to_task):
            try:
                zona_nome, data_corrente, stats_zona, log_data = future.result()
                completed_tasks += 1
                
                if completed_tasks % 50 == 0:
                    progress_msg = f"Completati {completed_tasks}/{len(tasks)} task ({completed_tasks/len(tasks)*100:.1f}%)"
                    print(progress_msg)
                    log_manager.log_info(progress_msg)
                
                # Scrivi log dettagliato per questa zona/giorno
                for durata, finestre_list in log_data.items():
                    if finestre_list:
                        log_manager.log_finestre_analizzate(
                            zona_nome,
                            data_corrente.strftime('%Y-%m-%d'),
                            durata,
                            finestre_list
                        )
                
                # Salva risultati
                for durata, valori in stats_zona.items():
                    if valori:
                        for stat in statistiche_attive:
                            if stat in valori and not np.isnan(valori[stat]):
                                chiave = f"{stat}_{durata}h"
                                data_str = data_corrente.strftime('%Y-%m-%d')
                                if data_str not in risultati_finali[chiave]:
                                    risultati_finali[chiave][data_str] = {}
                                risultati_finali[chiave][data_str][zona_nome] = valori[stat]
                                
            except Exception as e:
                task = future_to_task[future]
                zona_path = task[0]
                zona_nome = os.path.basename(zona_path).split('.')[0]
                data_corrente = task[1]
                error_msg = f"ERRORE nel future per zona {zona_nome}, data {data_corrente.strftime('%Y-%m-%d')}: {str(e)}"
                print(error_msg)
                log_manager.log_error(error_msg)
                completed_tasks += 1
    
    log_manager.log_info("")
    log_manager.log_info("=" * 100)
    log_manager.log_info(f"ELABORAZIONE COMPLETATA - Task completati: {completed_tasks}/{len(tasks)}")
    log_manager.log_info("=" * 100)
    log_manager.log_info("")
    
    # FASE 4: SALVATAGGIO RISULTATI BASE
    log_manager.log_info("FASE 4: Salvataggio risultati base")
    log_manager.log_info("-" * 100)
    
    try:
        dataframes_finali = {}
        for chiave, dati_dict in risultati_finali.items():
            if dati_dict:
                df = pd.DataFrame.from_dict(dati_dict, orient='index')
                df.index.name = 'Data'
                zone_ordinate = [z.split('.')[0] for z in zone_shapefiles]
                df = df.reindex(columns=zone_ordinate)
                df = df.sort_index()
                # Reset index per avere 'Data' come colonna
                df = df.reset_index()
                dataframes_finali[chiave] = df
        
        os.makedirs(config['output_directory'], exist_ok=True)
        output_path = os.path.join(config['output_directory'], config['output_filename'])
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for chiave, df in dataframes_finali.items():
                df.to_excel(writer, sheet_name=chiave, index=False)
        
        log_manager.log_info(f"File Excel base generato: {output_path}")
        log_manager.log_info("")
        
        # FASE 5: CALCOLO PEQ_0 (SE RICHIESTO)
        if config['statistiche'].get('Peq_0', False):
            log_manager.log_info("=" * 100)
            log_manager.log_info("FASE 5: Calcolo Peq_0 (Pioggia Equivalente)")
            log_manager.log_info("=" * 100)
            log_manager.log_info("")
            
            # Verifica presenza durata 24h
            if 24 not in durate:
                log_manager.log_warning("ATTENZIONE: Peq_0 richiesta ma durata 24h NON presente nella configurazione")
                log_manager.log_warning("Per calcolare Peq_0 è necessario includere 24 nelle durate_ore")
                log_manager.log_warning("Calcolo Peq_0 SALTATO")
                log_manager.log_info("")
            else:
                # Verifica presenza directory raster CN
                raster_cn_dir = config.get('raster_cn_directory')
                if not raster_cn_dir or not os.path.exists(raster_cn_dir):
                    log_manager.log_error("Directory raster CN non trovata o non specificata")
                    log_manager.log_error(f"  Path configurato: {raster_cn_dir}")
                    log_manager.log_error("  Calcolo Peq_0 SALTATO")
                    log_manager.log_info("")
                else:
                    log_manager.log_info(f"Directory raster CN: {raster_cn_dir}")
                    log_manager.log_info("")
                    
                    # Calcola CN per ogni zona
                    log_manager.log_info("Calcolo Curve Number per zone...")
                    zone_cn = calcola_cn_per_zone(raster_cn_dir)
                    
                    if not zone_cn:
                        log_manager.log_error("Nessun CN calcolato. Verificare i file raster")
                        log_manager.log_error("  Calcolo Peq_0 SALTATO")
                        log_manager.log_info("")
                    else:
                        log_manager.log_info(f"CN calcolati per {len(zone_cn)} zone")
                        log_manager.log_info("")
                        
                        # Estrai il DataFrame media_24h
                        chiave_24h = 'media_24h'
                        if chiave_24h not in dataframes_finali:
                            log_manager.log_error(f"Foglio {chiave_24h} non trovato nei risultati")
                            log_manager.log_error("  Calcolo Peq_0 SALTATO")
                            log_manager.log_info("")
                        else:
                            df_24h = dataframes_finali[chiave_24h].copy()
                            log_manager.log_info(f"Dati media_24h: {len(df_24h)} giorni")
                            
                            # Calcola cumulata 5 giorni
                            log_manager.log_info("Calcolo cumulata 5 giorni precedenti...")
                            cum_5d = calcola_cum_5d(df_24h)
                            log_manager.log_info("Cumulata 5d calcolata")
                            
                            # Calcola Peq_0
                            log_manager.log_info("Calcolo Peq_0 per tutte le zone...")
                            peq0_df = calcola_peq0_per_tutte_zone(cum_5d, zone_cn)
                            log_manager.log_info("Peq_0 calcolato")
                            log_manager.log_info("")
                            
                            # Crea fogli con Peq_0
                            log_manager.log_info("Creazione fogli con Peq_0...")
                            fogli_peq0 = {}
                            
                            # IMPORTANTE: Aggiungi sempre il foglio Peq0 base
                            fogli_peq0['Peq0'] = peq0_df
                            fogli_peq0['Cum_5d'] = cum_5d
                            
                            for durata in durate:
                                for stat in statistiche_attive:
                                    chiave_originale = f"{stat}_{durata}h"
                                    chiave_peq0 = f"{stat}_{durata}h_Peq0"
                                    
                                    if chiave_originale in dataframes_finali:
                                        df_originale = dataframes_finali[chiave_originale]
                                        df_con_peq0 = somma_statistiche_con_peq0(df_originale, peq0_df)
                                        fogli_peq0[chiave_peq0] = df_con_peq0
                                        log_manager.log_info(f"  Creato {chiave_peq0}")
                            
                            log_manager.log_info("")
                            
                            # Salva tutti i fogli aggiuntivi
                            log_manager.log_info("Salvataggio fogli aggiuntivi nel file Excel...")
                            
                            with pd.ExcelWriter(output_path, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
                                # Salva tutti i fogli Peq_0 (include Cum_5d e Peq0)
                                for chiave, df in fogli_peq0.items():
                                    df.to_excel(writer, sheet_name=chiave, index=False)
                                    log_manager.log_info(f"  Salvato foglio: {chiave}")
                            
                            log_manager.log_info("")
                            log_manager.log_info(f"Totale fogli Peq_0 aggiunti: {len(fogli_peq0)}")
                            log_manager.log_info(f"  - Cum_5d: {len(cum_5d)} righe")
                            log_manager.log_info(f"  - Peq0: {len(peq0_df)} righe")
                            log_manager.log_info(f"  - Fogli con somma Peq_0: {len(fogli_peq0) - 2}")
                            log_manager.log_info("")
        
        log_manager.log_info("=" * 100)
        log_manager.log_info("ANALISI COMPLETATA CON SUCCESSO")
        log_manager.log_info("=" * 100)
        log_manager.log_info(f"File Excel: {output_path}")
        log_manager.log_info(f"File Log: {log_manager.log_file}")
        
        print(f"\n=== ANALISI COMPLETATA ===")
        print(f"File Excel: {output_path}")
        print(f"File Log: {log_manager.log_file}")
        
    except Exception as e:
        error_msg = f"Errore nel salvataggio: {str(e)}"
        print(error_msg)
        log_manager.log_error(error_msg)


if __name__ == "__main__":

    main()
