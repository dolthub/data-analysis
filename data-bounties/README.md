## DoltHub data bounties

DoltHub data bounties are a way to make money while also

* learning about git
* learning about mysql
* building cool datasets
* and more

We have given out over $100,000 to date. There is no limit on the number of participants for any given bounty, and anyone is welcome to submit a pull request.

## The scoreboard

Dolt keeps track of who submits which cells. If you update or add a cell you get a point. If someone overwrites one of your cells, you lose a point. If someone adds the same data you already added, nothing happens. Points only update when data changes.

The scoreboard takes the total number of money and divides by the points (cell edits) for each person, to compute the amount we pay out.

> DoltHub data bounty money usually comes from our marketing budget, but sometimes comes from companies who ask us to collect data for them. We can crowdsource datasets that need a lot of man-hours and scraping expertise, on a medium-sized budget.


## Getting started

Start by installing Dolt: https://docs.dolthub.com/introduction/installation/

Dolt works just like Git, but with tables
```
dolt init
dolt table import -u <table> <your-data.csv>
dolt add .
dolt commit -am "some commit message"
dolt push origin main
```

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

## Keeping your PRs atomic: don't branch off of a branch

To keep your branches from being dependent on each other, check out main in between making branches.

1. `(branch: my_new_branch) dolt checkout main`
1. `(branch: main) dolt checkout -b my_second_branch`
1. `(branch: my_second_branch) dolt table import -u <table> <your-new-data.csv>`

and so on.

## Staying up to date with the main bounty repo

If we make changes to the bounty repo and you have an existing fork, doing `dolt pull` won't get those changes into your local copy. It'll just pull in the changes from your own fork, which doesn't have the changes from the bounty repo.

Here's an easy way to get those changes. 

1. Find the last common ancestor you have with the bounty repo. Copy the commit hash. Usually it's the commit that's labeled "initialize data repository"
1. do `dolt reset --hard {commit hash}`
1. `dolt add remote dolthub dolthub/{repo-name}`
1. `dolt pull dolthub main` (this will add those changes to your repo)
1. `dolt push origin main -f` (force push the changes to your remote copy)

## Code of Conduct

Trust, transparency, and attitude play a huge part in the success of producing a quality dataset via DoltHub's contest format. 

By participating, bounty hunters implicitly agree to the following **Code of Conduct**:

1. I will treat all participants and channel members with respect
1. I have checked my submissions for mistakes
1. The data that I submit is my own

Example violations of the Code of Conduct include:

* negative talk about other people in the channel
* regularly making low-quality submissions
* not following bounty instructions
* disguising another persons's submission as my own, or vice versa

and in general, anything else that passes the common-sense test. Ask yourself if it's worth the risk. And if you have to ask, the answer is probably "no."

Bounty hunters who violate the code of conduct won't be allowed to participate in bounties, and potentially forfeit their outstanding winnings, depending on the context. They may or may not be notified prior to taking action.

If you have questions, just DM me @spacelove on Discord.
