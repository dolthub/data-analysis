> If you're coming here from HN, this repo contains the scripts needed to reproduce the plot in the [Trillion Prices blog post](https://www.dolthub.com/blog/2022-09-02-a-trillion-prices/)

## If you were shocked to find

that United Healthcare, Aetna, and Anthem Blue Cross have their negotiated rates in humongous JSON files, well, so was I. Initially, I balked at the idea that these were processable on a normal machine, but with some trial and error (and some help from our Discord), we have a working flattener that will stream, filter, and flatten these files on a machine with < 2GB RAM (conservatively -- it's probably lower.)

At the moment, because of the complexity of the files, only filtering by billing code is supported. But we'll slowly flesh out the parser to handle more cases.

## Getting started
Try going to the `processors` folder and running

```
python example1.py
```

To get a feel for what the data output looks like. The file `example2.py` loops through a large index file, pulls out the in-network files, and scrapes them one by one. I don't recommend letting it run forever, but you can monitor resource use and see if it will work for your use-case.

## How it works
A few magical snippets and the package `ijson` do all of the work.

Streaming the GZipped files is done with:

```py
with requests.get(url, stream = True) as r:
	f = gzip.GzipFile(fileobj = r.raw)
```

Once you're streaming, you create a parser with `ijson`:

```py
parser = ijson.parse(f, use_float = True)
```

Then, you simply go through each row of the JSON line by line and parse it. (I had no idea this was possible.)

Streaming the file means you only get a chance to read each line once. This creates a problem since, occasionally, some of the later lines of the file (the negotiated rates) only make sense when given the providers that they reference, which are in the beginning of the file. Or, similarly, you may want to filter the billing codes, then only keep the provider references which are needed.

One way around this is to cache the provider references object and use it later.

```py
if row == ('provider_references', 'start_array', None):
	exist_provrefs = True
	provrefs, row = parse_provrefs(row, parser)
	provref_id_map = {r['provider_group_id']:i for i, r in enumerate(provrefs)}
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

The reasons for this are two-fold. Because we expect to build this database with distributed processing (and possibly even a DoltHub data bounty):

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
.filter(pl.col('Healthcare Provider Taxonomy Code_1') == '207V00000X')
.select(['NPI'])
.collect()).to_csv('obgyn_npi.csv')
```


## How you can help

1. Make a pull request to this repo to make this scraper better written and more robust
2. Help us design a good schema for these flattened files
3. Write a parser for Humana's CSV files
4. Add features (like filtering by NPI number -- how do we handle provider references in this case?)

## Plans to build the database on DoltHub

While we already know that the database is big -- and orders of magnitude too big for a DoltHub. But we're experimenting with this tool as we gear up for a possible data bounty to collect a subset of the data.

## Building this database with Charity Engine

[Charity Engine](https://charityengine.net/) has 100k machines that can process, store, and query this data. They're working with us at DoltHub to make the full database available for public use. We're looking for sponsors, so please reach out to me if you think this database could serve your business.