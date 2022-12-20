for table in root codes provider_groups negotiated_rates provider_groups_negotiated_rates_link;
do
  echo dolt table import -u $table $1/$table.csv
done