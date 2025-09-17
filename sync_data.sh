#!/bin/bash

# Synchronize the data folders that airflow updates remotely to
# the chosen local directory (to test dag updates)

# WARNING: THIS WILL DOWNLOAD MULTIPLE GBs of data!!

# Define local and remote paths
LOCAL="/data-read/qa4sm-airflow-data/"
REMOTE="qa4sms2:/qa4sm/data/"

# Define remote directories to sync
REMOTE_DIRS=(
    "C3S_combined/C3S_V202212-ext"
    "C3S_combined/C3S_V202312-ext"
    "ERA5/ERA5_latest-ext"
    "ERA5_LAND/ERA5_LAND_latest-ext"
    "SMOS_L2/SMOSL2_v700-ext"
)

# Loop through each remote directory and sync with rsync
for DIR in "${REMOTE_DIRS[@]}"; do
    echo "Syncing $REMOTE$DIR to $LOCAL"
    rsync -avz --progress --relative "$REMOTE./$DIR" "$LOCAL"
done

echo "All sync operations completed."
