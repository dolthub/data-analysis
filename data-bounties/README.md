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

## Teams and Discord

You can form a team and I can split the money evenly between all team members. Before a bounty forms, let @spacelove on Discord know who you'd like to be on your team. When that bounty period ends, I'll split any money earned by team members evenly before sending out checks.

It's strongly encouraged (though not yet enforced) that your Discord handle in [#data-bounties](https://discord.gg/sTXsQKKEHC) match your DoltHub username.

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

Trust, transparency, and attitude play a huge part in the success of producing a quality dataset via DoltHub's contest format. Bounty hunters are usually smart people making money in their spare time to get experience with data engineering or building cool datasets.

Having said that, by participating, bounty hunters implicitly agree to the following **Code of Conduct**:

1. I will treat all participants and channel members with politness and respect
3. I have checked my submissions for mistakes to the best of my ability
4. The data that I submit is my own and not someone else's
5. The identity that I provide upon payment is my own and not someone else's

Example violations of the Code of Conduct include:

* negative talk about other people in the channel
* regularly making low-quality submissions or ignoring instructions
* excessive arguing
* disguising another person's work or identity as one's own

and in general, anything else that passes the common-sense test. Ask yourself if it's worth the risk. And if you have to ask, the answer is probably "no."

Apart from @spacelove, no other DoltHub team members are involved in or influence the data bounties. They are engineering, fundraising, and handling paid customer services. It's an abuse of the DM system (and a general waste of time) to vent about data bounties to any of them. Do not do this.

If it comes to light that you're working closely with (or sharing winnings) with a banned member, it is very likely that you'll be banned yourself, and forfeit your winnings, without warning.

Bounty hunters who violate the code of conduct won't be allowed to participate in bounties, and potentially forfeit their outstanding winnings, depending on the context. They may or may not be banned from the Discord server. They may or may not be notified prior to taking action. 

If you have questions, just DM me @spacelove on Discord.
