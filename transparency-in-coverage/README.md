> If you're coming here from HN, this repo contains the scripts needed to reproduce the plot in the [Trillion Prices blog post](https://www.dolthub.com/blog/2022-09-02-a-trillion-prices/)

## For the context of this repo, see

1. https://www.dolthub.com/blog/2022-10-03-c-sections/
1. https://www.dolthub.com/blog/2022-09-02-a-trillion-prices

## If you were shocked to find

that United Healthcare, Aetna, and Anthem Blue Cross have their negotiated rates in humongous JSON files, so was I. My initial reaction is that these were unprocessable, but some discussion with friends online hinted that there might be a way to stream them.

This is that solution. With some trial and error (and some help from our Discord), we have a tool that will stream, filter, and flatten these files on a machine with <1GB of RAM, with a small caveat: the fewer NPI numbers you need, the faster this runs and the smaller the files it produces are. 

> This is not robust software, but it works acceptably well for some use-cases. 

In my limited testing RAM and CPU are not the bottlenecks -- bandwidth and disk space are. If you only need some CPT codes, parsing will go as fast as you can download the files. If you also filter down the NPI numbers you collect from you won't have space concerns either.

## Getting started

Try going to the `python/processors` folder and running

```
python example1.py
```

To get a feel for what the data output looks like. 

The file `example2.py` loops through a large index file, pulls out the in-network files, and scrapes them one by one. I don't recommend letting it run forever, but you can monitor resource use and see if it will work for your use-case.

The file `example3.py` will get the rates for C-sections from OB-GYNs and hospital NPIs. To run the script on a single file, do:

```sh
python example3.py -u https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Mississippi--Inc-_Insurer_HML-75_ED_in-network-rates.json.gz -o uhc_cesarean

```

You can run the shell script `c_section.sh` to spawn several of these processes in screens.

```sh
./c_section.sh
```

will output a folder with C-section rates from thousands of different providers. (Write me at alec@dolthub.com if you know of a better way.)

## How it works

A few magical snippets and the package `ijson` do all of the work. Streaming the GZipped files is done with:

```py
with requests.get(url, stream = True) as r:
	f = gzip.GzipFile(fileobj = r.raw)
```

Once you're streaming, you create a parser with `ijson`:

```py
parser = ijson.parse(f, use_float = True)
```

The parser streams tuples that represent each value in the JSON. 

Streaming sometimes creates a problem since often the the negotiated rates, which come at the end, only make sense when given the providers that they reference, which are at the beginning. So we cache the provider references in a `dict` and use them later.

```py
if (prefix, event) == ("provider_references", "start_array"):
    provrefs, row = build_provrefs(row, parser, npi_list)

    if provrefs:
    	# returns a dictionary that links provider_group_id
    	# to the provider_groups it contains
        provref_idx = provrefs_to_idx(provrefs)
    else:
        provref_idx = None
```

### Hashes as keys

Before we write each dict to file, we turn it into bytes and md5 hash it.

```py
def hashdict(data_dict):
	"""Get the hash of a dict (sort, convert to bytes, then hash)
	"""
	sorted_dict = dict(sorted(data_dict.items()))
	dict_as_bytes = json.dumps(sorted_dict).encode('utf-8')
	dict_hash = hashlib.md5(dict_as_bytes).hexdigest()
	return dict_hash
```

The reasons for this are two-fold:

1. Two machines should be able to process the same files and get the same keys. This rules out UUIDs.
2. Two machines should be able to process different files and get different keys. This rules out simple integers.

Write me at alec@dolthub.com if you know of a better way.

## Getting the NPI numbers for your use-case

If you need to get a list of NPI numbers, I recommend checking out the [CMS's database](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination
) of them. You can easily get a list and filter down to taxonomy by doing (for example)

```py
import polars as pl
(pl
.scan_csv('npidata_pfile_20050523-20220911.csv')
.filter(pl.col('Healthcare Provider Taxonomy Code_1') == '207V00000X') # OB/GYNs
.filter(pl.col('Entity Type Code') == '2') # only organizations, not individuals
.select(['NPI'])
.collect()).to_csv('obgyn_npi.csv')
```

## How you can help

1. Find bugs -- test this parser on a lot of different files and see where it breaks
2. Write a similar parser for Humana's CSVs that results in the same schema
3. Come talk to us on our Discord! https://discord.gg/GZXfE4jym3