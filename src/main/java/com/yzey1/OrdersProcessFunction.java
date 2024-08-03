package com.yzey1;

import org.apache.flink.api.java.tuple.Tuple2;

public class OrdersProcessFunction {

    public static Tuple2<String, Object[]> process(Object[] data) {
        System.out.println("Running OrdersProcessFunction class.");
        return new Tuple2<String, Object[]>("o", data);
    }
}
