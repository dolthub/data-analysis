# Convenience script for importing the table data in the right order
# Usage: bash import.sh <out_dir> 
# <out_dir> is where your root.csv (etc.) files are saved

for table in plans files plans_files codes provider_groups prices prices_provider_groups;
do
  echo WRITING TABLE $table
  dolt table import -u $table $1/$table.csv
done
