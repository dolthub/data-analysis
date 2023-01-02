# About `mrfutils`

`mrfutils.py` is a single file pythons script to help filter and flatten the enormous MRF files that come from different insurance payers. 

The amount of data in these files is staggering. MRFUtils will allow you to give it a list of billing codes and NPI numbers and then only take the prices from the MRF that match both the code and NPI.

The codes and NPIs should be passed as CSV files.

`codes.csv` looks something like
```
CPT,12345
CPT,12346
MS-DRG,123
```

and `npis.csv`
```
1234567890
0123453598
```
and so on. To run, first pick an `in-network-rates.json` file, and then do:

```bash
python3 example_cli.py --url <url> --out <output_dir> --codes <code_file_location> --npis <npi_file_location>
```

where the flags are
```
--npis <npifile.csv>
--codes <codesfile.csv>
--out <outputdirectory>
--url <mrf url>
```

### Q: Will this work on table of contents files or allowed-amounts files?
A: No. This **will not work** for _index.json_ or _allowed-amounts.json_ file. The script will fail. These files don't contain rates.  Index files do, however, contain links to files with rates. So you may want to write a program that loops through them and gets those files. `example2.py` shows you how to do that.

### Q: Can I contribute?
A: `mrfutils` is not seeking developers or maintainers, but having it work reliably is important. If you find a bug or a performance improvement, please let me know. Examples are welcome additions, but `mrfutils.py` should stay as simple as possible.

## Example

Pick an _in-network-rates.json_ file from one of the links below

* https://transparency-in-coverage.uhc.com/
* https://web.healthsparq.com/healthsparq/public/#/one/insurerCode=BSCA_I&brandCode=BSCA/machine-readable-transparency-in-coverage

(for the second one, you'll need to use the Web Developer Tools to get the URL directly.)

then run the above script.
