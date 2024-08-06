package com.yzey1;

import com.yzey1.DataTuple.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
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
        DataTuple tuple = value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
            aliveCount.update(0);
            prevTuple.update(null);
        }

        if (op_type.equals("+")){
            prevTuple.update((order) tuple);
            aliveCount.update(aliveCount.value() + 1);

        } else if (op_type.equals("-")) {
            prevTuple.update(null);
            aliveCount.update(aliveCount.value() - 1);
        }

        for (lineitem l : aliveTuples.value()) {
            out.collect(new Tuple2<>(op_type, getJoinedLineitem(prevTuple.value(), l)));
            System.out.println("Outputting: " + l.pk_value);
        }
    }

    @Override
    public void processElement2(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {

        String op_type = value.f0;
        DataTuple tuple = value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
        }
        if (aliveCount.value() == null) {
            aliveCount.update(0);
        }

//        String ID = tuple.getField("L_ORDERKEY").toString();
//        System.out.println("process element lineitem in lineitem process function");
//        System.out.println(ID);
//        System.out.println(aliveCount.value());

        if (checkCondition(tuple)) {
            if (op_type.equals("+")){
                aliveTuples.value().add((lineitem) tuple);
                System.out.println("Adding: " + tuple.pk_value);
                if (aliveCount.value() == 1) {
                    lineitem newoutput = getJoinedLineitem(prevTuple.value(), (lineitem) tuple);
                    out.collect(new Tuple2<>(op_type, newoutput));
                }
            } else if (op_type.equals("-")) {
                aliveTuples.value().remove((lineitem) tuple);
                System.out.println("Removing: " + tuple.pk_value);
                if (aliveCount.value() == 1) {
                    lineitem newoutput = getJoinedLineitem(prevTuple.value(), (lineitem) tuple);
                    out.collect(new Tuple2<>(op_type, newoutput));
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
        return l;
    }

}