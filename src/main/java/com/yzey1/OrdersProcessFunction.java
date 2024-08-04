package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class OrdersProcessFunction extends KeyedCoProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {
    @Override
    public void processElement1(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        out.collect(value);
    }

    @Override
    public void processElement2(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        out.collect(value);
    }

}