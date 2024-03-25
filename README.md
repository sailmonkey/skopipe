Simple Market Pipeline Build Instructions:


Python3.10 Installation:

 sudo add-apt-repository ppa:deadsnakes/ppa
 sudo apt update
 sudo apt install python3.10 python3.10-venv python3.10-dev

Create virtual environment and clone repo

git clone https://github.com/sailmonkey/skopipe.git
cd skopipe
python3.10 -m venv vpipe
source vpipe/bin/activate
pip install -r requirements.txt
Optional: “pip install jupyterlab”  notebook can be launch in screen “
Launch container apps:

Docker images:

********* Cassandra **********
docker network create cassandra

docker run --rm -d --name cassandra_skopipe --hostname cassandra_sko --network cassandra  -p 10.234.112.101:9042:9042 -p 10.234.112.101:9160:9160   -d cassandra

docker run --rm --network cassandra -v "$(pwd)/data.cql:/scripts/cass-setup.cql" -e CQLSH_HOST=cassandra_skopipe -e CfQLSH_PORT=9042 -e CQLVERSION=3.4.6 nuvo/docker-cqlsh
sudo snap install cqlsh
cqlsh
SELECT * FROM market.trades;
Default Credentials: cassandra / cassandra

If docker doesnt work install cqlsh on local host and run cqlsh 10.234.112.101
Copy and paste cassandra-setup.cql and paste into prompt.

CREATE KEYSPACE IF NOT EXISTS market 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE market;

CREATE TABLE IF NOT EXISTS trades(
    uuid uuid,
    symbol text,
    trade_conditions text,
    price double,
    volume double,
    trade_timestamp timestamp,
    ingest_timestamp timestamp,
    PRIMARY KEY((symbol),trade_timestamp))
WITH CLUSTERING ORDER BY (trade_timestamp DESC);

CREATE INDEX IF NOT EXISTS ON trades (uuid);


****** prometheus *******
docker run --rm -d --name prometheus_skopipe --network cassandra -p 10.234.112.101:9090:9090 -v /path/to/prometheus.yml:/etc/prometheus/prometheus.yml -v prometheus-data:/prometheus prom/prometheus

Or install local “apt install prometheus”

Launch node exporter on local host:
wget https://github.com/prometheus/node_exporter/releases/download/v1.7.0/node_exporter-1.7.0.linux-amd64.tar.gz

******** grafana ***********
docker run --rm -d --name grafanas_skopipe1 --hostname grafana_sko25 --network cassandra -p 10.234.112.101:3000:3000  grafana/grafana-enterpris

Default Credentials: admin / fbuser#123

DashBoard Json: https://github.com/sailmonkey/skopipe/blob/main/MarketData%20dashboard-1709761183616.json


Keys/Environmental Variables:

export FINNHUB_API_KEY=cngk6vpr01qq9hn92hq0cngk6vpr01qq9hn92hqg
echo $FINNHUB_API_KEY
export NYC_FB200_ACCESS_KEY=PSFBSAZQHGDJCEAFGHLPOLCNGBACAEDAODGJNMCF
echo $NYC_FB200_ACCESS_KEY
export NYC_FB200_SECRET_ACCESS_KEY=E4E8963E3402016/555c7B2D10A3ed4a863bGFNP
echo $NYC_FB200_SECRET_ACCESS_KEY

S3 endpoint URL: 10.234.112.148
FB API Token: T-4adb010c-bde3-4f30-9ce4-8294002bde6e

