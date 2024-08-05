package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;


public class OrdersProcessFunction extends KeyedCoProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {

    public ValueState<DataTuple> aliveTuple;
    public ValueState<Integer> aliveCount;
    public SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    public String DATE = "1993-10-01";

    // select tuple satisfying the where clause condition
    public boolean checkCondition(DataTuple tuple) throws ParseException {
        Date date = sdf.parse(DATE);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, 3);
        Date dateAfterThreeMonths = cal.getTime();

        // Parse the order date from the tuple
        Date orderDate = sdf.parse((String) tuple.getField("O_ORDERDATE"));

        // Check if the order date is between the parsed date and the date three months after
        return orderDate.after(date) && orderDate.before(dateAfterThreeMonths);
    }

    @Override
    public void processElement1(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        out.collect(value);
    }

    @Override
    public void processElement2(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
        out.collect(value);
    }

}