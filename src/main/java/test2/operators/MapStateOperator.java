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

package test2.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import org.apache.commons.lang3.StringUtils;
import test2.MyKey;
import test2.MyValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MapStateOperator extends AbstractStreamOperator<Tuple2<MyKey, MyValue>>
        implements OneInputStreamOperator<Tuple2<MyKey, MyValue>, Tuple2<MyKey, MyValue>>,
                BoundedOneInput {

    public static OutputTag<String> STATE_RESULT_TAG =
            new OutputTag<String>(Constants.OUTPUT_TAG_NAME) {};

    private final int totalRecords;

    private MapStateDescriptor<MyValue, Integer> mapStateDescriptor;
    private MapState<MyValue, Integer> mapState;

    public MapStateOperator(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.mapStateDescriptor = new MapStateDescriptor<>("state", MyValue.class, Integer.class);
        this.mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<MyKey, MyValue>> streamRecord) throws Exception {

        Integer current = mapState.get(streamRecord.getValue().f1);
        if (current == null) {
            mapState.put(streamRecord.getValue().f1, 1);
        } else {
            mapState.put(streamRecord.getValue().f1, current + 1);
        }

        output.collect(streamRecord);
    }

    @Override
    public void endInput() throws Exception {
        getKeyedStateBackend()
                .applyToAllKeys(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        mapStateDescriptor,
                        (key, value) -> {
                            List<String> values = new ArrayList<>();
                            value.iterator()
                                    .forEachRemaining(
                                            entry -> {
                                                values.add(
                                                        entry.getKey().getValue()
                                                                + ","
                                                                + entry.getValue());
                                            });

                            output.collect(
                                    STATE_RESULT_TAG,
                                    new StreamRecord<>(key + " " + StringUtils.join(values, " ")));
                        });
    }

    public static void checkResult(String fileName, int totalRecords, int numberOfKeys)
            throws IOException {
        Set<Integer> processedKeys = new HashSet<>();

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach(
                    line -> {
                        String[] parts = line.split(" +", 2);
                        int key = Integer.parseInt(parts[0]);
                        String value = parts[1];

                        if (processedKeys.contains(key)) {
                            throw new RuntimeException("Repeat keys: " + key);
                        }

                        processedKeys.add(key);

                        int maxValue = totalRecords / numberOfKeys;
                        if (key < totalRecords % numberOfKeys) {
                            maxValue += 1;
                        }

                        String expectedValue =
                                StringUtils.join(
                                        IntStream.range(0, maxValue)
                                                .boxed()
                                                .map(i -> i + "," + 1)
                                                .collect(Collectors.toList()),
                                        " ");
                        if (!value.equals(expectedValue)) {
                            throw new RuntimeException(
                                    "Value not match: key = "
                                            + key
                                            + " value = "
                                            + value
                                            + ", expected = "
                                            + expectedValue);
                        }
                    });
        }

        for (int i = 0; i < numberOfKeys; ++i) {
            if (!processedKeys.contains(i)) {
                throw new RuntimeException("Key not found: " + i);
            }
        }
    }
}
