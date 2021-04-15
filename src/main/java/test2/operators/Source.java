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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import test2.MyKey;
import test2.MyValue;

import java.util.ArrayList;
import java.util.Arrays;

public class Source extends RichSourceFunction<Tuple2<MyKey, MyValue>>
        implements CheckpointedFunction {

    private final int totalRecords;
    private final int numberOfKeys;

    private volatile boolean running = true;
    private ListState<Integer> nextIndexState;
    private int nextRecord;

    public Source(int totalRecords, int numberOfKeys) {
        this.totalRecords = totalRecords;
        this.numberOfKeys = numberOfKeys;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {
        this.nextIndexState =
                functionInitializationContext
                        .getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("next", Integer.class));

        if (nextIndexState.get().iterator().hasNext()) {
            nextRecord = nextIndexState.get().iterator().next();
        }
    }

    @Override
    public void run(SourceContext<Tuple2<MyKey, MyValue>> sourceContext) throws Exception {
        while (running && nextRecord < totalRecords) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(
                        new Tuple2<>(
                                new MyKey(nextRecord % numberOfKeys),
                                new MyValue(nextRecord / numberOfKeys)));
                nextRecord++;
            }

            Thread.sleep(2);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        nextIndexState.update(new ArrayList<>(Arrays.asList(nextRecord)));
    }
}
