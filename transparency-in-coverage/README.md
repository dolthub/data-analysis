# TODOs

1. Get a working JSON parser that can *filter by code*. This cuts down way on time needed to stream
2. Write a Humana parser that can put the CSV files in the same format as the JSON files

# Transparency in Coverage

This repo contains the scripts needed to reproduce the plot in the [Trillion Prices blog post](https://www.dolthub.com/blog/2022-09-02-a-trillion-prices/) (check the `size_of_in_network_files` folder.)

And, after a positive response from the community, we decided to take a crack at building this database ourselves. Or at least some of it. That work is going in the `parsers` folder.

We'll need a team to do this. And no matter how far we get, we'll learn something along the way.

## Plans to build the database on DoltHub

While we already know that the database is orders of magnitude too big to be put into Dolt, I've raised the question of whether it's possible to run a data bounty on subsets of the payor MRFs. The answer is: we don't know. We currently have a parser written that functionally works well enough. But it's about 100x slower than it needs to be. 

## Plans to build a database in general

A few organizations have reached out to us with plans to sponsor/help build this database with their resources. Stay tuned for announcements in our discord.