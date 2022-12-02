#!/bin/bash

set -x

cd /root || exit

apt-get update
apt-get install -y python3 python3-pip python3-dev tmux git vim default-libmysqlclient-dev

curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh > /tmp/install.sh && bash /tmp/install.sh

pip3 install --upgrade doltpy pandas geopandas numpy matplotlib mysql-connector-python jupyter lxml plotly statsmodels kaleido

curl -sSL https://repos.insights.digitalocean.com/install.sh -o /tmp/install.sh
bash /tmp/install.sh

mkdir /root/data

pushd /root/data || exit
#dolt clone dolthub/us-housing-prices-v2
dolt clone onefact/paylesshealth
popd || exit

cd /root || exit

curl -fsSL https://deb.nodesource.com/setup_18.x -o /tmp/install_node.sh
bash /tmp/install_node.sh
apt-get install -y gcc g++ make nodejs

npm install pm2 -g

jupyter notebook --generate-config 
sudo pm2 start "jupyter notebook --allow-root --ip=0.0.0.0"
sudo pm2 start 'dolt sql-server --data-dir /root/data -H 127.0.0.1 --user=rl --password=trustno1 --loglevel=trace'
pm2 save
pm2 startup systemd

swapoff -a
dd if=/dev/zero of=/swapfile bs=1G count=80
chmod 0600 /swapfile
mkswap /swapfile
swapon /swapfile
echo "/swapfile swap swap sw 0 0" >> /etc/fstab
