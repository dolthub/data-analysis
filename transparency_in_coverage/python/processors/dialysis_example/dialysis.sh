#!/bin/bash

for URL in $(cat urls.txt)
do
	screen -dm python3 process_dialysis.py -o dialysis --npi dialysis_npis.csv -u $URL
done
