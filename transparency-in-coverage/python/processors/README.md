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

#### Q: What python version do I need?
A: Right now it works best under python 3.9, since `ijson` has not released its high performance C backend for 3.10+ yet.

#### Q: How do I run this?
A: The only two files needed to start flattening the in-network `.json` files are `schema.py` and `mrfutils.py`. `example_cli.py` shows you the basics of what you need in order to parse these files. You can input either a local file or a remote URL. If you choose to import from local, you will need to pass the URL as a parameter.

#### Q: Will this work on table of contents files or allowed-amounts files?
A: This will not work for _index.json_ or _allowed-amounts.json_ file as these files don't contain rates.  Index files do, however, contain links to files with rates. So you may want to write a program that loops through them and gets those files. `example2.py` shows you how to do that.

#### Q: Can I contribute?
A: I'm not seeking developers but having the tool work reliably is important. If you find a bug or a potential performance improvement, please let me know. Extra examples, plugins, and unit tests are welcome additions, but `mrfutils.py` should stay as simple as possible.

## Example

Pick an _in-network-rates.json_ file from one of the links below

* https://transparency-in-coverage.uhc.com/
* https://web.healthsparq.com/healthsparq/public/#/one/insurerCode=BSCA_I&brandCode=BSCA/machine-readable-transparency-in-coverage

(for the second one, you'll need to use the Web Developer Tools to get the URL directly.)

then run the above script.
