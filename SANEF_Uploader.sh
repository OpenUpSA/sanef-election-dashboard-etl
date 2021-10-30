#!/usr/bin/env bash

set -euo pipefail

python SANEF_Uploader.py \
       $WAZI_ENDPOINT \
       $WAZI_TOKEN \
       $DATASET_ID \
       $IEC_TOKEN \
       $IEC_ENDPOINT \
       $DB_SERVER \
       $DB \
       $DB_USERNAME \
       $DB_PASSWORD \
       dont-reset
