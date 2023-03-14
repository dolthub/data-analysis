# ⚠️ This repo is actively being developed and changed ⚠️

Please don't rely on this repo for ongoing work. We're constantly integrating the lessons we learn into making this tool better, which means we're changing it constantly. 

Please let me know if you find any issues!

# About `mrfutils`

`mrfutils` is a python package to help filter and flatten the enormous MRF files that come from different insurance payers.

### Opinions

~~1. we pre-filter for `negotiated_type` = `"negotiated"` (this can be changed, but it's the default setting.) This is the amount the insurer directly pays the hospital for a service. It's easy to reason about. It's much harder to understand `"capitation"`, `"derived"`, `"bundle"` or `"fee schedule"` payments.~~
1. we pre-filter for `negotiated_type in ["negotiated", "fee schedule"]`. This recently changed. While we don't quite know what a "fee schedule" rate is, it seems that it's worth collecting the data, since there are far more "fee schedule" rates than negotiated rates. This discussion is ongoing. See here: https://github.com/CMSgov/price-transparency-guide/discussions/484
2. we have a `file_rate` table to track which rates come from which files. This table is for bookkeeping and can be deleted if you trust the data.
3. if a file has its `last_updated_on` anywhere but the top of the file, it's ignored. This was by design to make `mrfutils.py` easier to debug. Most files have it at the top, so this only affects a minority of them.

### To get started

If you plan on running any of the examples in the `examples/` folder, go to `mrfutils/` (this directory) and do

```
pip install -e .
pip install -r requirements.txt
```

to make sure that this package is installed as a development module with the right requirements.

Then go to `examples` and do:

```bash
python example_cli.py --file examplefile.json.gz --url 'http://example.com'
```
This should produce a debug output that explains what it's writing.

You'll want/need the `yajl2_c` backend to make `mrfutils` go fast. Do

```
brew install yajl
```

on Mac, (not sure about Windows instructions) or just comment out the line in `mrfutils.py` to avoid an `AssertionError`.


### The bare minimum

After installing the package all you really need to do is call `json_mrf_to_csv` from `mrfutils`:

```python
>> > from mrfutils import in_network_file_to_csv
```

with the right arguments. 

```python
def json_mrf_to_csv(
	url: str,
	out_dir: str,
	file:        str | None = None,
	code_filter: set | None = None, # not optional for the bounty
	npi_filter:  set | None = None, # not optional for the bounty
) -> None:
```

The examples show you how to pass in a list of NPIs (set of strings) and CPT codes (set of tuples).

### Specify codes and NPI numbers

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
python3 example_cli.py --url <url> --out-dir <output_dir> --code-file <code_file_location> --npi-file <npi_file_location>
```

where the flags are
```
--npi-file <npifile.csv>
--code-file <codesfile.csv>
--out <outputdirectory>
--url <mrf url>
```

### Handling index/table_of_contents files

If plan information isn't in the in-network file, then it's in an index file somewhere else. There's another tool in `mrfutils` called `toc_file_to_csv()` that you use the same way:
```python
toc_file_to_csv(url = 'http://my_index_file.com/thefileitself.json', out_dir = 'some_dir')
```
or with an optional `file` parameter (if you have the file saved to disk):
```python
toc_file_to_csv(file = 'local_file.json', url = 'http://my_index_file.com/thefileitself.json', out_dir = 'some_dir')
```
Note that you always have the pass the source URL.

### Importing

You can use the `import_in_network.sh` shell script to quickly import in-network files produced by `in_network_file_to_csv()`. The same goes for `import_toc.sh`.

#### Q: Why does `mrfutils` create so many duplicate rows in the CSVs?

`mrfutils` doesn't know what data its seen before and will write everything as it sees it. For example, it writes a TIN value every time it comes across one while writing a rate. On the one hand, this means it only writes what it uses. On the other, it means that what it does use, it can write multiple times. The only way to avoid duplicating the information on saving is to either rewrite the program logic or to use a database. 

If you're concerned about the size of the CSVs or duplicate rows, I recommend deduplicating using a dataframe library like pandas or polars after saving.

#### Q: What if I want to use a local file?
Pass the local file to `--file` and the url as `--url`. `mrfutils.py` will read from the local file but use the URL to get the filename. The URL that appears in the database will come from the URL. 

#### Q: What python version do I need?
A: Right now it works best under python 3.9, since `ijson` has not released its high performance C backend for 3.10+ yet.

#### Q: How do I run this?
A: The only two files needed to start flattening the in-network `.json` files are `schema.py` and `mrfutils.py`. `example_cli.py` shows you the basics of what you need in order to parse these files. You can input either a local file or a remote URL. If you choose to import from local, you will need to pass the URL as a parameter.

#### Q: Will this work on table of contents files or allowed-amounts files?
A: This will not work for _index.json_ or _allowed-amounts.json_ file as these files don't contain rates.  Index files do, however, contain links to files with rates. So you may want to write a program that loops through them and gets those files. `example2.py` shows you how to do that.

#### Q: Can I contribute?
A: This tool is open source but not necessarily open to contributions. However having the tool work reliably is important. If you find a bug or a potential performance improvement, please let me know. Extra examples, plugins, and unit tests are welcome additions, but `mrfutils.py` should stay as simple as possible. Bugs that have been found so far:
* price hash was miscomputed (including the filename hash and code hash as part of the hash)
* processing remote provider references would conk out (now processed as a stream)
* requests.get was fragile (replaced with Session.get)

## Example

Pick an _in-network-rates.json_ file from one of the links below

* https://transparency-in-coverage.uhc.com/
* https://web.healthsparq.com/healthsparq/public/#/one/insurerCode=BSCA_I&brandCode=BSCA/machine-readable-transparency-in-coverage

(for the second one, you'll need to use the Web Developer Tools to get the URL directly.)

then run the above script.
