package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AggregationProcessFunction extends KeyedProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {
    @Override
    public void processElement(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        out.collect(value);
    }
}