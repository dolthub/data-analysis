# Convenience script for importing the table data in the right order
# Usage: bash import.sh <out_dir> 
# <out_dir> is where your root.csv (etc.) files are saved

for table in file insurer code price_metadata rate npi_rate;
do
  echo WRITING TABLE $table
  dolt table import -u $table $1/$table.csv
done
