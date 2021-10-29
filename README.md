# sanef-election-dashboard-etl

    python SANEF_Uploader.py https://production.wazimap-ng.openup.org.za ...token... ...dataset_id... ...iec_api_token... ...iec_endpoint... ...db_server... ...db... ...db_username... ...db_password... ...reset_datatset...

## In Docker

Build

    docker build -t sanef-etl .

Run

    docker run --rm -ti -e WAZI_ENDPOINT=... -e WAZI_TOKEN=... -e DATASET_ID=... -e IEC_TOKEN=... -e IEC_ENDPOINT=... -e DB_SERVER=... -e DB=... -e DB_USERNAME=... -e DB_PASSWORD=... sanef-etl /app/SANEF_Uploader.sh

## iec_endpoints

- ward_votes_by_party - 1378
- voter_turnout - 1386
- ward_votes_by_candidate - 1379
- ward_councillor_elected - 1382

- pr_votes_by_party - 1380
- hung_councils - 1384
- seats_won - 1383