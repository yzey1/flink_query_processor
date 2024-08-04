package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import com.yzey1.DataTuple.nation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.w3c.dom.TypeInfo;

import java.util.HashSet;
import java.util.Set;

public class NationProcessFunction extends KeyedProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {

    private ValueState<HashSet<nation>> aliveTuples;
    private ValueState<Integer> aliveCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<HashSet<nation>> typeInformation = TypeInformation.of(new TypeHint<HashSet<nation>>() {});
        ValueStateDescriptor<HashSet<nation>> descriptor = new ValueStateDescriptor<>("Alive Nation Tuples", typeInformation);
        aliveTuples = getRuntimeContext().getState(descriptor);
    }

    public boolean isAlive(nation tuple) throws Exception {
        return true;
    }

    @Override
    public void processElement(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        System.out.println("Running NationProcessFunction class.");
        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
        }
        aliveTuples.value().add((nation) value.f1);
        out.collect(new Tuple2<String, DataTuple>("nation", value.f1));
    }
}
