# Parallel Processing

## Setup

- Clone this repository and go into it
- Make sure you have python >=3.10 installed
- Create a venv using `python -m venv venv`
- Install prod requirements with `pip install -r requirements.prod.txt`
- copy template.env to .env and change the values according to your needs:
  - `AZURE_STORAGE_SAS_URL` - SAS to access the blob storage - Make sure you generate a SAS with all permissions on Container/Blob objects. Expiration should be long enought to cover the expected load runtime.
  - `APPLICATIONINSIGHTS_CONNECTION_STRING` - Get the Application insights connection string if you want to monitor the workload processing performance and progress.

## First Execution Preperations

### Venv Activation

in the project folder run:

`source venv/bin/activate`

### Corpus Manifest
This file contains the names of all METS xml files of the corpus including the relative path to the root of the corpus blob container.

To check if the file exists and how many entries are in it run:

`python manifest.py report`

If the file (named by default "corpus.manifest.txt") does not exist in the root of the container, you can generate it with:

`python manifest.py generate`

If for any reason new data was added to the corpus - you can force-regeenrate this manifest using:

`python manifest.py generate --force_refresh`

By default generating the manifest will also upload it to the container (overrding any existing manifest by the same name). You can skip uploading for testing using the `--skip_upload` flag.
In theory - all modules support custom manifest file name using flags - usually you would stick to the default.

## Execution

- Always ensure the venv is active
- Starting the pipeline using the amount of workers suitable for the machine by:

`python parallel_processing.py`

- You can change parallelism if you need to using the `--num_processes` flag

- This process will download the required data from the corpus for the processed workload, and cleanup any local data no longer required.
- To make this process efficient, we limit the total files processed in a single run to 10, you can override this with `--max_files`. You can probably safely go up to 50 or 100 for longer processing runs.
- CSV Results are being uploaded automatically to the blob container under the output folder (named `output` by default) and also kept locally for ever under `output_uploaded`. This folder can be deleted from time to time to save space after ensuring all results have been uploaded successfully.
- Any processed entry from the corpus manifest is registered in the "processed manifest" which is also created and synced to the blob storage automatically.
- You can stop the run, and continue - it will pick up where it left.

## NOTE

This pipeline supports running from multiple machines, but will require a more advanced configuration to generate per-machine exclusing manifest file. Easier to just get a bigger machine.