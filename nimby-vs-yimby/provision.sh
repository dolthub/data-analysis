#!/bin/bash

set -x

cd /root || exit

apt-get update
apt-get install -y python3 python3-pip python3-dev tmux git vim default-libmysqlclient-dev

curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh > /tmp/install.sh && bash /tmp/install.sh

pip3 install --upgrade doltpy pandas geopandas numpy matplotlib mysql-connector-python jupyter lxml

curl -sSL https://repos.insights.digitalocean.com/install.sh -o /tmp/install.sh
bash /tmp/install.sh

mkdir /root/data

pushd /root/data || exit
dolt clone dolthub/us-housing-prices-v2
popd /root/data || exit

swapoff -a
dd if=/dev/zero of=/swapfile bs=1G count=16
chmod 0600 /swapfile
mkswap /swapfile
swapon /swapfile
echo "/swapfile swap swap sw 0 0" >> /etc/fstab

jupyter notebook --generate-config 
sudo tmux new-session -d -s jupyter "jupyter notebook --allow-root --ip=0.0.0.0"

cd /root/data && sudo tmux new-session -d -s dolt 'dolt sql-server -H 127.0.0.1 --user=rl --password=trustno1 --loglevel=trace'
