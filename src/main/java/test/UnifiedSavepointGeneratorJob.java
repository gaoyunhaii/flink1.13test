/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import test.operators.AggregationStateOperator;
import test.operators.ListStateOperator;
import test.operators.MapStateOperator;
import test.operators.ReduceStateOperator;
import test.operators.Source;
import test.operators.ValueStateOperator;

public class UnifiedSavepointGeneratorJob {

    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromArgs(args);

        int totalRecords = tool.getInt("total_records");
        int numberOfKeys = tool.getInt("num_keys");
        int parallelism = tool.getInt("parallelism");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(20, CheckpointingMode.EXACTLY_ONCE);

        String stateBackendName = tool.get("state_backend");
        switch (stateBackendName) {
            case "hashmap":
                env.setStateBackend(new HashMapStateBackend());
                break;
            case "rocksdb":
                env.setStateBackend(new EmbeddedRocksDBStateBackend());
                break;
            case "rocksdb_incre":
                env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
                break;
            default:
                throw new RuntimeException("Not supported statebackend " + stateBackendName);
        }

        String stateBackendPath = tool.get("state_backend_path");
        env.getCheckpointConfig().setCheckpointStorage(stateBackendPath);

        DataStream<Tuple2<Integer, Integer>> source =
                env.addSource(new Source(totalRecords, numberOfKeys)).uid("source");

        KeySelector<Tuple2<Integer, Integer>, Integer> keySelector = tuple -> tuple.f0;
        source.keyBy(keySelector)
                .transform("value state", source.getType(), new ValueStateOperator(totalRecords))
                .uid("value_state")
                .keyBy(keySelector)
                .transform(
                        "reducing state", source.getType(), new ReduceStateOperator(totalRecords))
                .uid("reducing_state")
                .keyBy(keySelector)
                .transform(
                        "aggregation state",
                        source.getType(),
                        new AggregationStateOperator(totalRecords))
                .uid("aggregating_state")
                .keyBy(keySelector)
                .transform("list state", source.getType(), new ListStateOperator(totalRecords))
                .uid("list_state")
                .keyBy(keySelector)
                .transform("map state", source.getType(), new MapStateOperator(totalRecords))
                .uid("map_state");

        env.execute();
    }
}
