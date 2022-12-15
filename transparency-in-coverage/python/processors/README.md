# Using the CLI tool

Pick a file labeled "in-network rates" from 

https://transparency-in-coverage.uhc.com/

or 

https://web.healthsparq.com/healthsparq/public/#/one/insurerCode=BSCA_I&brandCode=BSCA/machine-readable-transparency-in-coverage

**Do not choose a file labeled "index" or "allowed amounts".** The script will fail. Those files don't contain what we're looking for. 

I'll provide the codes and npis in two separate csv files, codes.csv and npis.csv. 

Then, with your codes.csv and npis.csv files, do

```bash
python3 example_cli.py --url <url> --out <your_dir> --codes <code_file_location> --npis <npi_file_location>
```

where the flags are 

```
--npis <npifile.csv>
--codes <codesfile.csv>
--out <outputdirectory>
--url <mrf url>
```

You can try it on https://mrf.healthsparq.com/bsca-egress.nophi.kyruushsq.com/prd/mrf/BSCA_I/BSCA/2023-01-01/inNetworkRates/2023-01-01_1116-1014-010212546_Blue-Shield-of-California.json.gz

## How to use this
1. Use the given `codes.csv` and `npis.csv` files 
2. Run the command above on different URLs
3. Import any tables that are produced. `root.csv` can be imported with `dolt table import -u root root.csv`, and the same for the other files.


## My open questions/TODOs
1. Should there be a foreign key relationship between tables?
2. 
