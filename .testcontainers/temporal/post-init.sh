#!/bin/sh
set -eu

NAMESPACE=${DEFAULT_NAMESPACE:-datashare-default}
TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-temporal:7233}

create_temporal_namespace() {
  echo "Creating namespace '$NAMESPACE'..."
  temporal operator namespace describe -n "$NAMESPACE" --address "$TEMPORAL_ADDRESS" || temporal operator namespace create -n "$NAMESPACE" --address "$TEMPORAL_ADDRESS"
  echo "Namespace '$NAMESPACE' created"
}

create_custom_search_attributes_namespace() {
  temporal operator search-attribute create --address "$TEMPORAL_ADDRESS" -n "$NAMESPACE" --name="UserId" --type="Keyword"
  temporal operator search-attribute create --address "$TEMPORAL_ADDRESS" -n "$NAMESPACE" --name="Progress" --type="Double"
  temporal operator search-attribute create --address "$TEMPORAL_ADDRESS" -n "$NAMESPACE" --name="MaxProgress" --type="Double"
}

create_temporal_namespace
create_custom_search_attributes_namespace