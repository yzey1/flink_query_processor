package com.yzey1;

import org.apache.flink.api.java.tuple.Tuple2;

public class LineitemProcessFunction {
    public static Tuple2<String, Object[]> process(Object[] data) {
        System.out.println("Running LineitemProcessFunction class.");
        return new Tuple2<String, Object[]>("l", data);
    }
}
