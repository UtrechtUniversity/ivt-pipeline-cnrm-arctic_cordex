# IVT Pipeline for CNRM CMIP6 → CORDEX Arctic

This repository was developed as part of the **PolarRES** project to support Arctic climate research.

It provides a complete processing pipeline to calculate **Integrated Vapor Transport (IVT)** from the **CNRM-ESM2-1** model data within the CMIP6 framework. The output is regridded to the **CORDEX Arctic rotated grid** for regional climate analysis.

The pipeline automates the following steps:

- Downloading polar region monthly 6-hourly data of specific humidity (`hus`), zonal wind (`ua`), and meridional wind (`va`) on 9 pressure levels.
- Calculating IVT from the downloaded variables.
- Wrapping the data and adding the polar point to handle grid singularities.
- Regridding the IVT data onto the CORDEX Arctic rotated grid.
- Archiving the final processed datasets for long-term storage.

This workflow ensures reproducible and streamlined IVT computation tailored for Arctic climate studies.

---

## Pipeline Overview

The workflow is controlled by two scripts:

### 1. `run_run_ivt_pipeline.sh`  
Submits multiple `run_ivt_pipeline.sh` Runs via SLURM, typically for sets of 3 months (adjustable).
This batching is designed to stay within the maximum 8-hour SLURM runtime.
Each job starts only after the previous one finishes, because the download step relies on OpenDAP, which is unstable when run in parallel across multiple jobs.

### 2. `run_ivt_pipeline.sh`  
Main pipeline script, called either interactively or through SLURM. This performs:
-  Data download and preprocessing
-  IVT calculation (via pressure level integration)
-  Regridding and wrapping
-  Archiving of results

---

## Folder Structure

<pre> ivt-pipeline-cnrm-arctic/
├── run_run_ivt_pipeline.sh         # SLURM batch launcher for multiple jobs
├── run_ivt_pipeline.sh            # Core pipeline for a single month or time slice
├── Download_process_cmip6.py      # Downloads CNRM CMIP6 6-hourly data via CDS API
├── calculate_ivt.sh               # Calculates IVT from Q, U, V on pressure levels
├── Regrid_RotPolar_CORDEX.py      # Regrids to CORDEX Arctic rotated grid & wraps pole
├── wrap_and_add_pole_allvars.py   # Helper to wrap all variables across dateline/pole
├── archive_IVT_cnrmCORDEX.sh      # Archives the processed files to tape (ECMWF specific)
├── Level-bounds.py                # Creates level_bounds.nc for vertical interpolation
├── level_bounds.nc                # Predefined bounds for hybrid → pressure conversion
├── example_grid.nc                # Target CORDEX Arctic grid </pre>

---

##  Requirements

- Python 3.12+
- CDO, NCO (with `HDF5_USE_FILE_LOCKING=FALSE`)
- CDS API key (for downloading CMIP6 data)
- SLURM for batch processing (optional)

Install Python packages:
`pip install xarray netCDF4 numpy cdsapi`


## How to Run the Pipeline
### Option 1: Run single month manually
````bash
./run_ivt_pipeline.sh CNRMESM21 1985 1985 02 02
````

### Option 2: Submit as SLURM job
````bash
sbatch run_ivt_pipeline.sh CNRMESM21 1985 1985 02 02
````

### Option 3: Batch multiple months via `run_run_ivt_pipeline.sh`
Edit the launcher script to loop over your desired months, then:
````bash
sbatch run_run_ivt_pipeline.sh
````
## ⚠️ Important: Be Careful with Parallel Runs and File Cleanup

**Warning:**  
The pipeline’s automatic deletion of CMIP6 input files older than two years has been **removed from the Python download script**. Instead, cleanup is performed **manually at the end of the bash wrapper script** to manage disk space.

### Important considerations:

- When running pipeline batches over different year ranges **in parallel** (e.g., 2016 and 2019 simultaneously), one batch may finish earlier and delete files still needed by another batch, causing incomplete or corrupted outputs.
- To avoid this:
  - Run pipeline batches **sequentially**, waiting for one to finish before starting the next.
  - If parallel runs are required, **disable cleanup during runs** by leaving the deletion command commented out in the bash wrapper.
  - Then, after all batches complete, **manually enable cleanup** by uncommenting these lines near **lines 168–169** in `run_ivt_pipeline.sh`:

    ```bash
    echo ">>> Cleaning up all files in ssp370_files directory ..."
    rm -v $MODEL_INPUT/ssp370_files/*
    ```

## Processing Steps (per month)

1. Download CNRM CMIP6 data from CDS (via Download_process_cmip6.py)

2. Calculate IVT using the 9 pressure levels (300 → 1000 hPa)

3. Regrid and wrap to CORDEX Arctic using Regrid_RotPolar_CORDEX.py

4. Archive results to tape or backup storage

## Notes

The pipeline uses only publicly available CMIP6 data from the CDS API.
NorESM versions are not included here due to private data access.
All outputs are monthly NetCDF files on the CORDEX rotated grid.
The system is designed for flexibility and SLURM batch scaling.

## Environment Notes
This pipeline was developed and tested on the ECMWF Atos HPC system, using the SLURM workload manager.

Some components (e.g. tape archiving, module loading) are ECMWF-specific. You may need to adapt paths or environment settings when running this pipeline on other systems.

## Contact
For questions or issues, contact:

- **Developer**: L. Gavras-van Garderen, <l.gavras-vangarderen@uu.nl>
- **Lead**: W. J. van de Berg, <w.j.vandeberg@uu.nl>

