#!/usr/bin/env python
# coding: utf-8

import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
import os
from dask.distributed import Client, LocalCluster
import dask 
import argparse
import subprocess
import glob
import concurrent.futures
import logging
import threading
log_lock = threading.Lock()


# Chunking functions
def generate_chunks_hus(year):
    return [
        (f"{year}01010600", f"{year}07010000"),
        (f"{year}07010600", f"{year+1}01010000"),
    ]

def generate_chunks_ua_va(year):
    return [
        (f"{year}01010600", f"{year}05010000"),
        (f"{year}05010600", f"{year}09010000"),
        (f"{year}09010600", f"{year+1}01010000"),
    ]

chunk_generators = {
    'hus': generate_chunks_hus,
    'ua': generate_chunks_ua_va,
    'va': generate_chunks_ua_va,
        }


def safe_log(message):
    with log_lock:
        logging.info(message)

def download_file_if_missing(url, output_path):
    file_name = os.path.basename(output_path)

    if os.path.exists(output_path):
        if os.path.getsize(output_path) < 10 * 1024 * 1024:  # 10 MB threshold
            safe_log(f"⚠️ File {file_name} appears corrupted or too small. Re-downloading.")
            os.remove(output_path)
        else:
            safe_log(f"✔️ File {file_name} already exists and looks valid.")
            return

    safe_log(f"⬇️ Downloading {file_name} from {url}")
    try:
        subprocess.run(
            ['wget', '-q', '-O', output_path, url],  # -q = quiet mode
            check=True
        )
        safe_log(f"✅ Successfully downloaded {file_name}")
    except subprocess.CalledProcessError as e:
        safe_log(f"❌ Failed to download {file_name} from {url}: {e}")
        raise

def delete_files_ending_in_year(local_dir, cutoff_year):
    for fname in os.listdir(local_dir):
        if f"{cutoff_year}" in fname:
            try:
                os.remove(os.path.join(local_dir, fname))
                print(f"🧹 Deleted old file: {fname}")
            except Exception as e:
                print(f"⚠️ Could not delete {fname}: {e}")
                
def interpolate_to_pressure_levels(data, p_full, target_pressures):
    """
    Interpolate data (e.g., q, u, v) from model pressure levels to standard pressure levels.

    Parameters:
    - data: xarray.DataArray with dims (time, lev, lat, lon)
    - p_full: pressure field with same dims (time, lev, lat, lon)
    - target_pressures: 1D numpy array of pressure levels (in Pa) to interpolate to

    Returns:
    - Interpolated data with dims (time, plev, lat, lon)
    """
    # Rechunk lev dimension to a single chunk
    data = data.chunk({'lev': -1})
    p_full = p_full.chunk({'lev': -1})

    def interp_1d(p, x, new_p):
        return np.interp(new_p, p[::-1], x[::-1])  # reverse for ascending order

    interpolated = xr.apply_ufunc(
        interp_1d,
        p_full,
        data,
        input_core_dims=[["lev"], ["lev"]],
        output_core_dims=[["plev"]],
        output_sizes={"plev": len(target_pressures)},
        vectorize=True,
        dask="parallelized",
        output_dtypes=[data.dtype],
        kwargs={"new_p": target_pressures},
    )

    interpolated = interpolated.assign_coords(plev=target_pressures)
    return interpolated

def align_and_interpolate(data_var, p_full, target_pressures):
    common_times = np.intersect1d(data_var.time.values, p_full.time.values)
    data_sel = data_var.sel(time=common_times)
    p_sel = p_full.sel(time=common_times)
    return interpolate_to_pressure_levels(data_sel, p_sel, target_pressures)



def main():
    #surpress unneeded logging in logfile
    logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

    # parse arguments
    parser = argparse.ArgumentParser(description="Process CMIP6 data")
    parser.add_argument("--gcm", type=str, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()
    
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    
    # SLURM env
    n_cpus = int(os.environ.get('SLURM_CPUS_ON_NODE', 8))
    mem_bytes = int(os.environ.get('SLURM_MEM_PER_NODE', 128 * 1024 ** 3))
    mem_gb = mem_bytes // (1024 ** 3)
    
    n_workers = 4
    threads_per_worker = max(1, n_cpus // n_workers)
    mem_per_worker = int(mem_gb * 0.85 // n_workers)
    
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=f'{mem_per_worker}GB'
    )
    client = Client(cluster)
    print(f"Dask client dashboard link: {client.dashboard_link}")
    print(client)
    
    dask.config.set({'distributed.comm.tcp.timeout.connect': '60s'})
    dask.config.set({'distributed.comm.tcp.timeout.read': '300s'})
    
    year = args.year
    month = args.month
    scenario_start_year = 2015
    target_pressures = np.array([300,400,500,600,700,750,850,925,1000], dtype=float)*100
    LAT_MIN=42
    
    experiment_config = {
        "historical": {
            "base_url_template": "https://esgf.ceda.ac.uk/thredds/dodsC/esg_cmip6/CMIP6/CMIP/CNRM-CERFACS/CNRM-ESM2-1/{experiment}/r1i1p1f2/6hrLev/",
            "end_url": "/gr/v20181206/"
        },
        "ssp370": {
            "base_url_template": "http://esg1.umr-cnrm.fr/thredds/fileServer/CMIP6_CNRM/ScenarioMIP/CNRM-CERFACS/CNRM-ESM2-1/{experiment}/r1i1p1f2/6hrLev/",
            "end_url": "/gr/v20191021/"
        }
    }
    
    file_template = "{var}_6hrLev_CNRM-ESM2-1_{experiment}_r1i1p1f2_gr_{start}-{end}.nc"
    
    print(f"\n=== Processing {year}-{month:02} ===")
    
    current_month_start_str = f"{year}-{month:02}-01"
    if month == 12:
        next_month_start_str = f"{year+1}-01-01"
    else:
        next_month_start_str = f"{year}-{month+1:02}-01"
    
    experiment = "historical" if year < scenario_start_year else "ssp370"
    config = experiment_config[experiment]
    
    q_month, u_month, v_month = None, None, None
    last_va_url_for_pressure_vars = None
    
    # Prepare downloads for ssp370 files if needed
    download_tasks = []
    local_dir = os.path.join(output_dir, 'ssp370_files')
    if experiment == "ssp370":
        os.makedirs(local_dir, exist_ok=True)
        for var in ['hus', 'ua', 'va']:
            chunks = chunk_generators[var](year)
            for chunk_start, chunk_end in chunks:
                chunk_start_dt = pd.to_datetime(chunk_start, format="%Y%m%d%H%M")
                chunk_end_dt = pd.to_datetime(chunk_end, format="%Y%m%d%H%M")
                target_period_start_dt = pd.to_datetime(current_month_start_str)
                target_period_end_dt = pd.to_datetime(next_month_start_str)
    
                if not (chunk_end_dt > target_period_start_dt and chunk_start_dt < target_period_end_dt):
                    continue
    
                file_name = file_template.format(var=var, experiment=experiment, start=chunk_start, end=chunk_end)
                remote_url = config["base_url_template"].format(experiment=experiment) + var + config["end_url"] + file_name
                local_path = os.path.join(local_dir, file_name)
    
                download_tasks.append((remote_url, local_path))
    
        # Download in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(download_file_if_missing, url, path) for url, path in download_tasks]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Download failed: {e}")
    
    # Now proceed to open and process datasets
    for var in ['hus', 'ua', 'va']:
        chunks = chunk_generators[var](year)
        var_data_for_month_list = []
    
        for chunk_start, chunk_end in chunks:
            try:
                chunk_start_dt = pd.to_datetime(chunk_start, format="%Y%m%d%H%M")
                chunk_end_dt = pd.to_datetime(chunk_end, format="%Y%m%d%H%M")
                target_period_start_dt = pd.to_datetime(current_month_start_str)
                target_period_end_dt = pd.to_datetime(next_month_start_str)
    
                if not (chunk_end_dt > target_period_start_dt and chunk_start_dt < target_period_end_dt):
                    continue
    
                base_url = config["base_url_template"].format(experiment=experiment)
                file_name = file_template.format(var=var, experiment=experiment, start=chunk_start, end=chunk_end)
    
                if experiment == "ssp370":
                    ds_chunk_path = os.path.join(local_dir, file_name)
                else:
                    ds_chunk_path = base_url + var + config["end_url"] + file_name
    
                if var == 'va':
                    last_va_url_for_pressure_vars = ds_chunk_path
    
                print(f"→ Opening {var} from {ds_chunk_path} for {year}-{month:02}")
    
                ds_chunk = None
                try:
                    ds_chunk = xr.open_dataset(ds_chunk_path, chunks={'time': 'auto', 'lat': 'auto', 'lon': 'auto', 'lev': 'auto'})
                    effective_slice_start = max(chunk_start_dt, target_period_start_dt)
                    effective_slice_end = min(chunk_end_dt, target_period_end_dt)
    
                    slice_start_str = effective_slice_start.strftime('%Y-%m-%d %H:%M:%S')
                    if effective_slice_end == target_period_end_dt:
                        slice_end_str = (effective_slice_end - pd.Timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        slice_end_str = effective_slice_end.strftime('%Y-%m-%d %H:%M:%S')
    
                    ds_selected_month_in_chunk = ds_chunk.sel(time=slice(slice_start_str, slice_end_str))
                    ds_selected_month_in_chunk = ds_selected_month_in_chunk.where(ds_selected_month_in_chunk.lat >= LAT_MIN, drop=True)
    
                    if not ds_selected_month_in_chunk.time.size:
                        print(f"  No data for {year}-{month:02} in this chunk of {var}.")
                        continue
    
                    ds_selected_month_in_chunk = ds_selected_month_in_chunk.sortby(['time', 'lat', 'lon'])
                    var_data_for_month_list.append(ds_selected_month_in_chunk[var])
    
                except Exception as e:
                    print(f"❌ Failed loading or processing {var} from {ds_chunk_path}: {e}")
    
                finally:
                    if ds_chunk is not None: ds_chunk.close()
    
            except Exception as e:
                print(f"❌ Error parsing chunk dates or general chunk processing for {var}: {e}")
    
        if var_data_for_month_list:
            concatenated_var_data = xr.concat(var_data_for_month_list, dim='time').sortby('time')
            _, index = np.unique(concatenated_var_data['time'], return_index=True)
            concatenated_var_data = concatenated_var_data.isel(time=index)
            if var == 'hus': q_month = concatenated_var_data
            elif var == 'ua': u_month = concatenated_var_data
            elif var == 'va': v_month = concatenated_var_data
        else:
            print(f"⚠️ No data found for {var} for {year}-{month:02} after processing all chunks.")
    
    # Open last va URL dataset to get pressure variables
    with xr.open_dataset(last_va_url_for_pressure_vars, chunks={'time':'auto','lat':'auto','lon':'auto'}) as ds_p:
        ps = ds_p['ps'].sel(time=slice(current_month_start_str, next_month_start_str))
        ps = ps.where(ps.lat >= LAT_MIN, drop=True)
        ap = ds_p['ap']
        b = ds_p['b']

        ap_4d = ap.expand_dims({'time': ps.time, 'lat': ps.lat, 'lon': ps.lon}).transpose('time', 'lev', 'lat', 'lon')
        b_4d = b.expand_dims({'time': ps.time, 'lat': ps.lat, 'lon': ps.lon}).transpose('time', 'lev', 'lat', 'lon')
        ps_4d = ps.expand_dims({'lev': ap_4d.lev}, axis=1).transpose('time', 'lev', 'lat', 'lon')

        p_full = ap_4d + b_4d * ps_4d

        vars_dict = {'q': q_month, 'u': u_month, 'v': v_month}
        for var_name, data_var in vars_dict.items():
            data_on_plev = align_and_interpolate(data_var, p_full, target_pressures)
            ds_out = xr.Dataset({var_name: data_on_plev})
            ds_out = ds_out.assign_coords({
                'plev': target_pressures,
                'time': data_on_plev.time,
                'lat': data_on_plev.lat,
                'lon': data_on_plev.lon,
            })
            ds_out['plev'].attrs.update({
                'units': 'Pa',
                'standard_name': 'air_pressure',
                'axis': 'Z',
                'positive': 'down'
            })

            output_filename = f"{var_name}_{year}{month:02}_Arctic_cnrm_9plev.nc"
            output_path = os.path.join(output_dir, output_filename)
            ds_out.to_netcdf(output_path)
            print(f"Saved {var_name} data to {output_path}")

    # delete files from more than two years ago, if that year is labelled in file name
    if experiment == "ssp370":
        local_dir = os.path.join(output_dir, 'ssp370_files')
        delete_files_ending_in_year(local_dir, year - 2)

    
    print("\nClosing Dask client.")
    client.close()
    cluster.close()

if __name__ == "__main__":
    main()

