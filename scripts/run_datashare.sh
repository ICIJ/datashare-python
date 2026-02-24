#!/bin/bash
datashare --mode SERVER \
  --batchQueueType TEMPORAL \
  --messageBusAddress temporal:7233 \
  --taskRoutingStrategy GROUP \
  --authFilter org.icij.datashare.session.YesCookieAuthFilter \
  --dataSourceUrl "jdbc:postgresql://localhost/datashare?user=datashare&password=datashare" \
  --defaultProject test-project \
  --elasticsearchAddress http://localhost:9200 \
  --queueType REDIS \
  --redisAddress redis://localhost:6379 \
  --rootHost http://localhost:8080 \
  --sessionStoreType REDIS \
  --sessionTtlSeconds 43200 \
  --tcpListenPort 8080
