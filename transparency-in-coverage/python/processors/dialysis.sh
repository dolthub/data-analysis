#!/bin/bash

for URL in $(cat urls.txt)
do
	screen -dm python3 example4.py -o dialysis --npi data/dialysis_and_hospital_npis.csv -u $URL
done
