#!/bin/bash -e

TAG=${1}

if [[ ${TAG} == "" ]]; then
   TAG=`date +"%Y-%m-%d-%H-%M-%S"`
fi

export CLOUDSDK_CORE_PROJECT=eoscanada-shared-services

echo "Using TAG: ${TAG}"
gcloud builds submit . \
        --config cloudbuild.yaml \
        --timeout 15m \
        --substitutions SHORT_SHA=${TAG}
