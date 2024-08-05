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
import java.util.Map;

public class CustomerProcessFunction extends KeyedCoProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {

    public ValueState<HashSet<customer>> aliveTuples;
    public ValueState<Integer> aliveCount;
    public ValueState<nation> prevTuple;

    // select tuple satisfying the where clause condition
    public boolean checkCondition(DataTuple tuple) {
        return true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        aliveTuples = getRuntimeContext().getState(new ValueStateDescriptor<>("Alive Customer Tuples", TypeInformation.of(new TypeHint<HashSet<customer>>() {})));
        aliveCount = getRuntimeContext().getState(new ValueStateDescriptor<>("Alive Customer Count", Integer.class));
        prevTuple = getRuntimeContext().getState(new ValueStateDescriptor<>("Previous Nation Tuple", TypeInformation.of(new TypeHint<nation>() {})));
    }

    @Override
    public void processElement1(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        System.out.println("Running CustomerProcessFunction class.");
        String op_type = value.f0;
        DataTuple tuple = value.f1;

        if (aliveCount.value() == null) {
            aliveCount.update(0);
        }

        if (op_type.equals("+")){
            prevTuple.update((nation) tuple);
            aliveCount.update(aliveCount.value() + 1);

        } else if (op_type.equals("-")) {
            prevTuple.clear();
            aliveCount.update(0);
        }

        if (aliveTuples.value() != null) {
            for (customer c : aliveTuples.value()) {
                out.collect(new Tuple2<>(op_type, getJoinedCustomer(prevTuple.value(), c)));
            }
        }
    }

    @Override
    public void processElement2(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        System.out.println("Running CustomerProcessFunction class.");
        String op_type = value.f0;
        DataTuple tuple = value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
        }
        if (aliveCount.value() == null) {
            aliveCount.update(0);
        }

        if (checkCondition(tuple) && aliveCount.value() == 1) {
            if (op_type.equals("+")){
                aliveTuples.value().add((customer) tuple);
            } else if (op_type.equals("-")) {
                aliveTuples.value().remove((customer) tuple);
            }
            out.collect(new Tuple2<>(op_type, getJoinedCustomer(prevTuple.value(), (customer) tuple)));
        }
    }

    public customer getJoinedCustomer(nation nation, customer customer) {
        customer joinedCustomer = new customer(customer);
        joinedCustomer.setField("N_NAME", nation.getField("N_NAME"));
        return joinedCustomer;
    }

}
