package com.yzey1;

import com.yzey1.DataTuple.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;


public class LineitemProcessFunction extends KeyedCoProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {

    public ValueState<HashSet<lineitem>> aliveTuples;
    public ValueState<Integer> aliveCount;
    public ValueState<order> prevTuple;

    // select tuple satisfying the where clause condition
    public boolean checkCondition(DataTuple tuple) {
        return (Objects.equals(tuple.getField("L_RETURNFLAG"), "R"));
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        aliveTuples = getRuntimeContext().getState(new ValueStateDescriptor<>("Alive Lineitem Tuples", TypeInformation.of(new TypeHint<HashSet<lineitem>>() {})));
        aliveCount = getRuntimeContext().getState(new ValueStateDescriptor<>("Alive Lineitem Count", Integer.class));
        prevTuple = getRuntimeContext().getState(new ValueStateDescriptor<>("Previous Order Tuple", TypeInformation.of(new TypeHint<order>() {})));
    }

    @Override
    public void processElement1(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {

        String op_type = value.f0;
        order tuple = (order) value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
            aliveCount.update(0);
            prevTuple.update(null);
        }

        if (aliveCount.value().equals(0) && op_type.equals("+")){
            prevTuple.update(tuple);
            aliveCount.update(aliveCount.value() + 1);
            for (lineitem l : aliveTuples.value()) {
                out.collect(new Tuple2<>(op_type, getJoinedLineitem(tuple, l)));
            }
        }

        if (aliveCount.value().equals(1) && op_type.equals("-")){
            prevTuple.update(null);
            aliveCount.update(aliveCount.value() - 1);
            for (lineitem l : aliveTuples.value()) {
                out.collect(new Tuple2<>(op_type, getJoinedLineitem(tuple, l)));
            }
        }
    }

    @Override
    public void processElement2(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {

        String op_type = value.f0;
        lineitem tuple = (lineitem) value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
            aliveCount.update(0);
            prevTuple.update(null);
        }

        if (checkCondition(tuple)) {
            if (op_type.equals("+")){
                aliveTuples.value().add(tuple);
                if (aliveCount.value() == 1) {
                    out.collect(new Tuple2<>(op_type, getJoinedLineitem(prevTuple.value(), tuple)));
                }
            } else if (op_type.equals("-")) {
                aliveTuples.value().remove(tuple);
                if (aliveCount.value() == 1) {
                    out.collect(new Tuple2<>(op_type, getJoinedLineitem(prevTuple.value(), tuple)));
                }
            }
        }
    }

    public lineitem getJoinedLineitem(order o, lineitem l) {
        l.setField("N_NAME", o.getField("N_NAME"));
        l.setField("C_CUSTKEY", o.getField("C_CUSTKEY"));
        l.setField("C_NAME", o.getField("C_NAME"));
        l.setField("C_ACCTBAL", o.getField("C_ACCTBAL"));
        l.setField("C_ADDRESS", o.getField("C_ADDRESS"));
        l.setField("C_PHONE", o.getField("C_PHONE"));
        l.setField("C_COMMENT", o.getField("C_COMMENT"));

        List<String> groupByFields = Arrays.asList("C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_ADDRESS", "N_NAME", "C_PHONE", "C_COMMENT");
        // set the foreign key value to the concatenated string of the group by fields
        StringBuilder concatenatedFields = new StringBuilder();
        for (String field : groupByFields) {
            String v = (String) l.getField(field);
            concatenatedFields.append(v).append(",");
        }
        // Remove the trailing comma
        if (concatenatedFields.length() > 0) {
            concatenatedFields.setLength(concatenatedFields.length() - 1);
        }
        String output_fields = concatenatedFields.toString();
        l.setField("output_fileds", output_fields);

        return l;
    }

}