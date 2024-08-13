package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import com.yzey1.DataTuple.nation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class NationProcessFunction extends KeyedProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {

    private ValueState<HashSet<nation>> aliveTuples;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<HashSet<nation>> typeInformation = TypeInformation.of(new TypeHint<HashSet<nation>>() {});
        ValueStateDescriptor<HashSet<nation>> aliveTupleDescriptor = new ValueStateDescriptor<>("Alive Nation Tuples", typeInformation);
        aliveTuples = getRuntimeContext().getState(aliveTupleDescriptor);
    }


    // select tuple satisfying the where clause condition
    public boolean checkCondition(DataTuple tuple) {
        return true;
    }

    @Override
    public void processElement(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
//        System.out.println("Running NationProcessFunction class.");
        String op_type = value.f0;
        nation tuple = (nation) value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
        }

        boolean isValid = false;
        if (checkCondition(tuple)) {
            if (op_type.equals("+")){
                isValid = aliveTuples.value().add(tuple);
            } else if (op_type.equals("-")) {
                isValid = aliveTuples.value().remove(tuple);
            }
            if (isValid) {
                out.collect(new Tuple2<>(op_type, tuple));
            }
        }
    }
}
