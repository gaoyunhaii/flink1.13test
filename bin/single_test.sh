#!/usr/bin/env bash

FLINK_BASE=${1}
START_BACKEND=${2-"hashmap"}
RESTORE_BACKEND=${3-"hashmap"}
START_PARALLELISM=${4-"10"}
RESTORE_PARALLELISM=${5-"10"}
TOTAL_RECORDS=${6-"30000"}
NUM_KEYS=${7-"1000"}

mkdir -p result
rm nohup.out
rm -rf savepoint/*
rm -rf state_backend/*
rm -rf result/*

nohup ${FLINK_BASE}/bin/flink run -c test.UnifiedSavepointGeneratorJob flink1.13test-1.0-SNAPSHOT.jar \
  --total_records ${TOTAL_RECORDS} \
  --num_keys ${NUM_KEYS} \
  --parallelism ${START_PARALLELISM} \
  --state_backend ${START_BACKEND} \
  --state_backend_path "file://${PWD}/state_backend" &
sleep 1
cat nohup.out
sleep $((5 + $RANDOM % 30))

job_id=$(${FLINK_BASE}/bin/flink list 2>/dev/null | grep "RUNNING" | awk '{print $4}')
${FLINK_BASE}/bin/flink stop --savepointPath "${PWD}/savepoint" ${job_id}

fg 1 # wait till the job finished

# now let's start the restore job and check the result
savepoint_dir=$(ls savepoint)
${FLINK_BASE}/bin/flink run --fromSavepoint "${PWD}/savepoint/${savepoint_dir}" \
  -c test.UnifiedSavepointRestartAndCheckJob flink1.13test-1.0-SNAPSHOT.jar \
  --total_records ${TOTAL_RECORDS} \
  --num_keys ${NUM_KEYS} \
  --parallelism ${RESTORE_PARALLELISM} \
  --state_backend ${RESTORE_BACKEND} \
  --state_backend_path "file://${PWD}/state_backend_restore" \
  --result_path "${PWD}/result"
