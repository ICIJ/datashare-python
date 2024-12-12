#!/bin/bash
datashare --mode SERVER \
  --batchQueueType AMQP \
  --taskRoutingStrategy GROUP \
  --authFilter org.icij.datashare.session.YesCookieAuthFilter \
  --dataSourceUrl "jdbc:postgresql://localhost/datashare?user=datashare&password=password" \
  --defaultProject test-project \
  --elasticsearchAddress http://localhost:9200 \
  --messageBusAddress amqp://guest:guest@localhost:5672 \
  --queueType REDIS \
  --redisAddress redis://localhost:6379 \
  --rootHost http://localhost:8080 \
  --sessionStoreType REDIS \
  --sessionTtlSeconds 43200 \
  --tcpListenPort 8080
