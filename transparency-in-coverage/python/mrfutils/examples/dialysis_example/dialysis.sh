#!/bin/bash

for URL in $(cat urls.txt)
do
	screen -dm python3 process_dialysis.py --out dialysis_csv --npi dialysis_npis.csv --url $URL
done
