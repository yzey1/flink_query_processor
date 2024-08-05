package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import com.yzey1.DataTuple.customer;
import com.yzey1.DataTuple.lineitem;
import com.yzey1.DataTuple.order;
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

        if (aliveCount.value() == null) {
            aliveCount.update(0);
        }

        System.out.println("process element 1");
        System.out.println(value.f1.getField("N_NAME"));
        System.out.println(aliveCount.value());

        if (op_type.equals("+")){
            prevTuple.update((order) tuple);
            aliveCount.update(aliveCount.value() + 1);

        } else if (op_type.equals("-")) {
            prevTuple.clear();
            aliveCount.update(0);
        }

        if (aliveTuples.value() != null) {
            for (lineitem l : aliveTuples.value()) {
                out.collect(new Tuple2<>(op_type, getJoinedLineitem(prevTuple.value(), l)));
            }
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

        System.out.println("process element 2");
        System.out.println(value.f1.getField("C_NAME"));
        System.out.println(aliveCount.value());

        if (checkCondition(tuple) && aliveCount.value() == 1) {
            if (op_type.equals("+")){
                aliveTuples.value().add((lineitem) tuple);
            } else if (op_type.equals("-")) {
                aliveTuples.value().remove((lineitem) tuple);
            }
            out.collect(new Tuple2<>(op_type, getJoinedLineitem(prevTuple.value(), (lineitem) tuple)));
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