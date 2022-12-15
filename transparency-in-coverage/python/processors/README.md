# Using the CLI tool

Pick a file labeled "in-network rates" from 

https://transparency-in-coverage.uhc.com/

or 

https://web.healthsparq.com/healthsparq/public/#/one/insurerCode=BSCA_I&brandCode=BSCA/machine-readable-transparency-in-coverage

Do not choose a file labeled "index" or "allowed amounts".

Then, with your codes.csv and npis.csv files, do

```bash
python3 example_cli.py --url https://mrf.healthsparq.com/bsca-egress.nophi.kyruushsq.com/prd/mrf/BSCA_I/BSCA/2023-01-01/inNetworkRates/2023-01-01_1116-1014-010212546_Blue-Shield-of-California.json.gz --out mydir --codes quest/codes.csv --npis quest/npis.csv
```

where the flags are 

```
--npis <npifile.csv>
--codes <codesfile.csv>
--out <outputdirectory>
--url <mrf url>
```