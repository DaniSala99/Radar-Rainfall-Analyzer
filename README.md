# Radar-Rainfall-Analyzer
Python-based radar precipitation analysis with configurable time windows, parallel processing, Equivalent Rainfall (Peq0) calculation, and comprehensive logging for hydrological studies.

## Key Features

### Total Flexibility
- **Customizable durations**: Analyze any time interval from 1 to 24 hours with completely free configuration
- **Versatile territorial zones**: Supports any shapefile for territorial clipping without predefined format constraints
- **Modular statistics**: Calculate only the necessary metrics among mean, median, percentiles (75th, 95th, 99th), min/max, standard deviation and Peq₀
- **Temporal scalability**: Process periods from single days to decades of historical data

### Advanced Technology
- **Parallel processing**: Automatic workload optimization on multiprocessing with intelligent resource management
- **Archive integrity verification**: Automatic check of missing and corrupted files with detailed reports
- **Moving windows**: "Sum first, clip after" algorithm for maximum computational efficiency
- **Complete logging system**: Detailed tracking of every operation with rankings and statistics

### Robustness
- **Advanced error handling**: Automatic management of damaged or missing files without interrupting processing
- **Multiple format support**: Timestamps with second precision (yyyyMMddHHmmss)
- **Memory optimization**: Automatic resource cleanup and intelligent temporary file management

## System Requirements

### Python Dependencies
```
python >= 3.7
rasterio >= 1.2.0
geopandas >= 0.10.0
numpy >= 1.20.0
pandas >= 1.3.0
shapely >= 1.8.0
psutil >= 5.8.0
openpyxl >= 3.0.0
```

### Recommended Hardware Requirements
- **CPU**: Multi-core processor (minimum 4 cores, recommended 8+ for extended processing)
- **RAM**: Minimum 8 GB (recommended 16+ GB for long-period analyses)
- **Storage**: Sufficient space for radar archive and temporary files
- **Operating System**: Windows/Linux/macOS with multiprocessing support

## Installation

### 1. Clone the repository
```bash
git clone <repository-url>
cd radar-pluviometrico-analyzer
```

### 2. Create virtual environment (optional but recommended)
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

## Configuration

The system uses a JSON file for complete analysis configuration.

### config.json Structure

```json
{
  "data_inizio": "2016-04-01",
  "data_fine": "2025-09-30",
  "durate_ore": [6, 12, 24],
  "statistiche": {
    "media": true,
    "mediana": false,
    "75_perc": false,
    "95_perc": false,
    "99_perc": false,
    "max": false,
    "min": false,
    "std": false,
    "Peq_0": true
  },
  "shapefile_directory": "path/to/zone_omogenee",
  "output_directory": "path/to/output",
  "output_filename": "Risultati_analisi.xlsx",
  "log_directory": "path/to/output/logs",
  "raster_cn_directory": "path/to/CN_raster"
}
```

### Configuration Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `data_inizio` | string | Analysis start date (format YYYY-MM-DD) |
| `data_fine` | string | Analysis end date (format YYYY-MM-DD) |
| `durate_ore` | array | Customizable array of durations to analyze (1-24 hours) |
| `statistiche` | object | Configurable statistics module: enable/disable each metric |
| `shapefile_directory` | string | Path to directory containing zone shapefiles |
| `output_directory` | string | Output directory for results |
| `output_filename` | string | Output Excel file name |
| `log_directory` | string | Directory for log files |
| `raster_cn_directory` | string | Path to Curve Number rasters (required only if Peq_0: true) |

### Available Statistics

| Statistic | Key | Description |
|-----------|-----|-------------|
| Arithmetic mean | `media` | Mean rainfall value over the zone |
| Median | `mediana` | Median value of the distribution |
| 75th percentile | `75_perc` | 75th percentile value |
| 95th percentile | `95_perc` | 95th percentile value |
| 99th percentile | `99_perc` | 99th percentile value |
| Maximum | `max` | Maximum recorded value |
| Minimum | `min` | Minimum recorded value |
| Standard deviation | `std` | Value dispersion |
| Equivalent Rainfall | `Peq_0` | Calculation according to NRCS-CN model |

## Configuration Examples

### Case 1: Short-Duration Intense Events Analysis
```json
{
  "durate_ore": [1, 2, 3],
  "statistiche": {
    "media": true,
    "max": true,
    "95_perc": true,
    "Peq_0": false
  }
}
```

### Case 2: Complete Multi-Duration Analysis
```json
{
  "durate_ore": [3, 6, 12, 18, 24],
  "statistiche": {
    "media": true,
    "mediana": true,
    "95_perc": true,
    "max": true,
    "Peq_0": true
  }
}
```

### Case 3: Equivalent Rainfall Focus
```json
{
  "durate_ore": [6, 12, 24],
  "statistiche": {
    "media": true,
    "Peq_0": true
  }
}
```

## Usage

### Basic Execution
```bash
python Analisi_file_radar.py config.json
```

### Typical Workflow

1. **Preparation**: Ensure radar archive is accessible
2. **Configuration**: Customize `config.json` according to needs
3. **Execution**: Launch the script
4. **Verification**: Check log file for any issues
5. **Analysis**: Open generated Excel file

### Execution Monitoring

During execution, the script provides real-time feedback:
- Archive integrity verification with valid/corrupted/missing file count
- Automatically optimized parallel process configuration
- Progress bar every 50 completed tasks
- Final summary with processing statistics

## Generated Output

### Main Excel File

The system generates a multi-sheet Excel file:

#### Base Sheets
- `media_Xh`: Mean for X-hour duration
- `mediana_Xh`: Median for X-hour duration
- `75_perc_Xh`, `95_perc_Xh`, `99_perc_Xh`: Percentiles
- `max_Xh`, `min_Xh`: Extreme values
- `std_Xh`: Standard deviation

#### Peq₀ Sheets (if enabled)
- `Cum_5d`: Cumulative rainfall from previous 5 days
- `Peq0`: Equivalent Rainfall values
- `media_Xh_Peq0`: Statistic + Peq₀ for each duration

### Data Structure

| Data | IM_01 | IM_02 | IM_03 | ... |
|------|-------|-------|-------|-----|
| 2016-04-01 | 15.3 | 18.7 | 12.4 | ... |
| 2016-04-02 | 8.2 | 10.5 | 7.8 | ... |

Values in millimeters (mm). Empty cells indicate missing data.

## Logging System

### Archive Integrity Report

```
==================================================================================
ARCHIVE INTEGRITY REPORT
==================================================================================

Days analyzed: 3471
Valid files: 81234
Corrupted/damaged files: 12
Missing files: 245
```

### Moving Windows Rankings Log

```
================================================================================
ZONE: IM_01 | DATE: 2020-05-10 | DURATION: 6h
--------------------------------------------------------------------------------
Total windows analyzed: 19

RANKING BY MEAN (descending order):

  1. Window: 2020-05-10 09:00 → 2020-05-10 15:00
     Mean: 45.23 mm
     Max: 58.4 | Min: 32.1 | Median: 44.8
```

## Equivalent Rainfall Calculation (Peq₀)

### Methodology

Implementation of the generalized NRCS-CN model:

```
S = 25400/CN - 254
M = √(S × (P₅ + ((1-λ)/2)² × S)) - ((1+λ)/2) × S
Peq₀ = M × (1 + λS/(S+M))
```

Where:
- **P₅**: Cumulative rainfall from previous 5 days (mm)
- **CN**: Zone mean Curve Number
- **λ**: Lambda parameter (default 0.2)

### Requirements

1. Set `"Peq_0": true` in statistics
2. Include 24h duration in `durate_ore`
3. Provide `raster_cn_directory` with ASC files

### CN Raster Format

- `.ASC` format (ASCII Grid)
- Filename ending with zone number
- Valid CN values (30-100)
- Same geographic projection as shapefiles

## Troubleshooting

### "Radar directory not found"
Check the `RADAR_DATA_PATH` path (line 33 of code).

### "No valid files found"
Check integrity report in log for details.

### "Peq₀ calculation error"
Verify:
1. 24h duration present in configuration
2. CN raster directory correct
3. Raster file format (.ASC)
4. Filenames with recognizable zone number

### "Out of Memory"
Solutions:
- Reduce temporal range
- Reduce number of durations
- Disable unnecessary statistics
- Increase available RAM

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Customize configuration
cp config.json my_config.json
# Edit my_config.json

# 3. Run analysis
python Analisi_file_radar.py my_config.json

# 4. Check results
# - Excel in output_directory
# - Log in log_directory
```

## License

System developed for Regional Functional Center - Radar Rainfall Data Analysis.

Use dedicated to civil protection agencies, functional centers and hydrological/meteorological research institutes.
