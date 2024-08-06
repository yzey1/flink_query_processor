package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class AggregationProcessFunction extends KeyedProcessFunction<String, Tuple2<String, DataTuple>, String> {

    ValueState<Double> currentValue;
    List<String> groupByFields = Arrays.asList("C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_PHONE", "N_NAME", "C_ADDRESS", "C_COMMENT");
    String aggregationField = "revenue";

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        currentValue = getRuntimeContext().getState(new ValueStateDescriptor<>("Old Value", Double.class));
    }

    @Override
    public void processElement(Tuple2<String, DataTuple> value, Context ctx, Collector<String> out) throws Exception {

        String op_type = value.f0;
        DataTuple tuple = value.f1;

        if(currentValue.value() == null) {
            currentValue.update(0.0);
        }

        double l_extendedprice = Double.parseDouble(tuple.getField("L_EXTENDEDPRICE").toString());
        double l_discount = Double.parseDouble(tuple.getField("L_DISCOUNT").toString());
        Double delta = l_extendedprice * (1 - l_discount);

        if (op_type.equals("+")) {
            currentValue.update(currentValue.value() + delta);
        } else if (op_type.equals("-")) {
            currentValue.update(currentValue.value() - delta);
        }

        // concat each field value in groupByFields and the delta revenue and current revenue
        StringBuilder result = new StringBuilder();
        for (String field : groupByFields) {
            result.append(field).append(": ").append(tuple.getField(field)).append(",");
        }
        result.append("Delta revenue: ").append(delta).append(",").append("Current revenue: ").append(currentValue.value());

        out.collect(result.toString());
    }
}