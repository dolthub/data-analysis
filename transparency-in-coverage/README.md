# TODOs

1. Get a working JSON parser that can *filter by code*. This cuts down way on streaming speed.
2. Write a Humana parser that can put the CSV files in the same format as the JSON files

# Transparency in Coverage

This repo contains the scripts needed to reproduce the plot in the [Trillion Prices blog post](https://www.dolthub.com/blog/2022-09-02-a-trillion-prices/) (check the `size_of_in_network_files` folder.)

And, after a positive response from the community, we decided to take a crack at building this database ourselves. Or at least some of it. That work is going in the `parsers` folder.

We'll need a team to do this. And no matter how far we get, we'll learn something along the way.

## Plans to build the database on DoltHub

While we already know that the database is orders of magnitude too big to be put into Dolt, I've raised the question of whether it's possible to run a data bounty on subsets of the payor MRFs. The answer is: we don't know. We currently have a parser written that functionally works well enough. But it's about 100x slower than it needs to be. 

Copied from that file: 

```
JSON Parser for the payor in-network files

Current issues (Sept. 23, 2022):

1. On my (@spacelove/Alec Stein) M1 Macbook Pro, processing a 1.3GB
JSON file takes about 11 minutes. I thought that writing CSV files
line-by-line might be the bottleneck, but when I didn't write 
_anything_, it still took 6 minutes just to stream the whole JSON
 file.

This is unacceptably long. At this rate, it would take CPU years just
to process all the JSON MRFs.

We need this to be about 100x faster. 

2. My test showed that the flat CSV files total to about the same size
as the original JSON file. There's no free size reduction just for
making them relational. 

Out of curiosity I decided to check what was taking up most of the
space.

Without the UUIDs, there is a size reduction of 25% in the largest
file (prices).

With the UUIDs, parquet format reduces the size about 85%. 

Without the UUIDs and in parquet, the size reduction is about 96%.
(Interestingly, this is still 2x the size of the compressed JSON --
apparently quite a light format.)

This is all a problem for us at DoltHub since Dolt will only work with
data on the order of ~X00GB, and we need some kind of linking number
ID (a hash, or a UUID) and we won't get the compression that you get
with parquet.

Since the data is at least 100TB we have to reduce what we store by a
factor of at least 99.9%. There are some options:

    1. limit the procedures we collect (there are at least 20,000).
    BUT: streaming the files is still too slow. 2. limit the NPIs
    that we collect. Same issue as above. 3. find a more efficient
    storage format/schema

3. UUIDs were supposed to solve the problem of distributed collection.
Basically, if two people use incrementing IDs as linking numbers,
they'll end up overwriting each other's  data. (If person A gets the
same linking numbers 1, 2, 3... as person B, then when person B goes
to insert a row with primary key (1), they'll overwrite person A's
primary key (1).

With UUIDs this can't happen and collisions are negligibly rare. On
the other hand, it's very easy to get duplicate rows with UUIDs,
which is its own problem. Hashing the rows somehow and using that as
a PK might be the way to go -- but how, and what, to hash?

4. We can probably come up with better column names than the ones
given here. We also need to check that these columns actually fit the
data. The SCHEMA below does not understand bundled codes, for
example -- we need to add that flexibilty.

5. There is one more TODO here. When provider references are given as
a URL, an async function needs to fetch the provider information from
that URL. ```

## Plans to build a database in general

A few organizations have reached out to us with plans to sponsor/help build this database with their resources. Stay tuned for announcements in our discord.