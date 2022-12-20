# Convenience script for importing the table data in the right order
# Usage: bash import.sh <out_dir> 
# <out_dir> is where your root.csv (etc.) files are saved

for table in root codes provider_groups negotiated_rates provider_groups_negotiated_rates_link;
do
  echo dolt table import -u $table $1/$table.csv
done
