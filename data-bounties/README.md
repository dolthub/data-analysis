## Dolthub data bounties

Dolthub data bounties are a way to make money while also

* learning about git
* learning about mysql
* building cool datasets
* and more

We have given out over $100,000 to date. There is no limit on the number of participants for any given bounty, and anyone is welcome to submit a pull request.

## Workflows

Start by reading the bounty README. When you feel like you've understood the data being collected, make a fork of the database. The remote copy, the one hosted on DoltHub, is called `origin`. 

To make changes to your fork, here's the typical workflow. Download some data to CSV. Then do

1. `dolt checkout -b my_new_branch`
1. `dolt table import -u <table> <your-data.csv>`
1. `dolt commit -am "a commit message that explains the additions"`
1. `dolt push origin main` (or just `dolt push`)

That will get changes on your `origin`. To get your data into our main branch you'll make a pull request.

Go to the bounty repo page and click the "Pull Requests" tab.

Make a pull request by selecting your remote and branch. Then write a short description in the description box explaining your changes.
