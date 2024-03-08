# skopipe
FSA Team Demo

### Docker Images ###
docker run --rm -d --name cassandra_skopipe --hostname cassandra_sko --network cassandra  -p 10.234.112.101:9042:9042 -p 10.234.112.101:9160:9160   -d cassandra
docker run --rm -d --name prometheus_skopipe --network cassandra -p 10.234.112.101:9090:9090 -v /path/to/prometheus.yml:/etc/prometheus/prometheus.yml -v prometheus-data:/prometheus prom/prometheus


### Updated packages with versions ###
pip install -r requirements.txt

### Must set the environmental variables before running Producer ###
export FINNHUB_API_KEY=<key>
export NYC_FB200_ACCESS_KEY=<key>
export NYC_FB200_SECRET_ACCESS_KEY=<key>
