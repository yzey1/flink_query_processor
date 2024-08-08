package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class AggregationProcessFunction extends KeyedProcessFunction<String, Tuple2<String, DataTuple>, Tuple9<String, String, Double, String, String, String, String, Double, Double>> {

    ValueState<Double> currentValue;
    List<String> groupByFields = Arrays.asList("C_CUSTKEY", "C_NAME", "C_ACCTBAL", "C_ADDRESS", "N_NAME", "C_PHONE", "C_COMMENT");
    String aggregationField = "revenue";

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        currentValue = getRuntimeContext().getState(new ValueStateDescriptor<>("Old Value", Double.class));
    }

    @Override
    public void processElement(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple9<String, String, Double, String, String, String, String, Double, Double>> out) throws Exception {

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

        // Use HashMap to store the groupByFields and the delta revenue and current revenue
        HashMap<String, Object> resultMap = new HashMap<>();
        for (String field : groupByFields) {
            resultMap.put(field, tuple.getField(field));
        }
        resultMap.put("deltaRevenue", delta);
        resultMap.put("currentRevenue", currentValue.value());

//        // Convert HashMap to string representation
//        String result = resultMap.toString();
//        out.collect(result);

        // Convert HashMap to tuple representation (for output csv)
        Tuple9<String, String, Double, String, String, String, String, Double, Double> result = new Tuple9<>(
                resultMap.get("C_CUSTKEY").toString(),
                resultMap.get("C_NAME").toString(),
                Double.parseDouble(resultMap.get("C_ACCTBAL").toString()),
                resultMap.get("C_ADDRESS").toString(),
                resultMap.get("N_NAME").toString(),
                resultMap.get("C_PHONE").toString(),
                resultMap.get("C_COMMENT").toString(),
                (Double) resultMap.get("deltaRevenue"),
                (Double) resultMap.get("currentRevenue")
        );
        // covert Tuple9 to string
        String str_result = result.toString();
        // remove the parentheses
        str_result = str_result.substring(1, str_result.length() - 1);

        out.collect(result);


    }
}