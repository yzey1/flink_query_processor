package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import com.yzey1.DataTuple.customer;
import com.yzey1.DataTuple.nation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class CustomerProcessFunction extends KeyedCoProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {

    public ValueState<HashSet<customer>> aliveTuples;
    public ValueState<Integer> aliveCount;

    // select tuple satisfying the where clause condition
    public boolean checkCondition(DataTuple tuple) {
        return true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<HashSet<customer>> typeInformation = TypeInformation.of(new TypeHint<HashSet<customer>>() {});
        ValueStateDescriptor<HashSet<customer>> aliveTupleDescriptor = new ValueStateDescriptor<>("Alive Customer Tuples", typeInformation);
        aliveTuples = getRuntimeContext().getState(aliveTupleDescriptor);
        aliveCount = getRuntimeContext().getState(new ValueStateDescriptor<>("Alive Customer Count", Integer.class));
    }

    @Override
    public void processElement1(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        out.collect(value);
    }

    @Override
    public void processElement2(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        out.collect(value);
    }

}
