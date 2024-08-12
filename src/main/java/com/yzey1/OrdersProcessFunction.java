package com.yzey1;

import com.yzey1.DataTuple.DataTuple;
import com.yzey1.DataTuple.customer;
import com.yzey1.DataTuple.nation;
import com.yzey1.DataTuple.order;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;


public class OrdersProcessFunction extends KeyedCoProcessFunction<String, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>, Tuple2<String, DataTuple>> {

    public ValueState<HashSet<order>> aliveTuples;
    public ValueState<Integer> aliveCount;
    public ValueState<customer> prevTuple;

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
        return (orderDate.after(date) && orderDate.before(dateAfterThreeMonths)) || orderDate.equals(date);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        aliveTuples = getRuntimeContext().getState(new ValueStateDescriptor<>("Alive Order Tuples", TypeInformation.of(new TypeHint<HashSet<order>>() {})));
        aliveCount = getRuntimeContext().getState(new ValueStateDescriptor<>("Alive Order Count", Integer.class));
        prevTuple = getRuntimeContext().getState(new ValueStateDescriptor<>("Previous Customer Tuple", TypeInformation.of(new TypeHint<customer>() {})));
    }

    @Override
    public void processElement1(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {

        String op_type = value.f0;
        customer tuple = (customer) value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
            aliveCount.update(0);
            prevTuple.update(null);
        }

        if (aliveCount.value().equals(0) && op_type.equals("+")){
            prevTuple.update(tuple);
            aliveCount.update(aliveCount.value() + 1);
            for (order o : aliveTuples.value()) {
                out.collect(new Tuple2<>(op_type, getJoinedOrder(tuple, o)));
            }
        }

        if (aliveCount.value().equals(1) && op_type.equals("-")){
            prevTuple.update(null);
            aliveCount.update(aliveCount.value() - 1);
            for (order o : aliveTuples.value()) {
                out.collect(new Tuple2<>(op_type, getJoinedOrder(tuple, o)));
            }
        }

    }

    @Override
    public void processElement2(Tuple2<String, DataTuple> value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {

        String op_type = value.f0;
        order tuple = (order) value.f1;

        if (aliveTuples.value() == null) {
            aliveTuples.update(new HashSet<>());
        }
        if (aliveCount.value() == null) {
            aliveCount.update(0);
        }

        if (checkCondition(tuple)) {
            if (op_type.equals("+")){
                aliveTuples.value().add(tuple);
                if (aliveCount.value() == 1) {
                    out.collect(new Tuple2<>(op_type, getJoinedOrder(prevTuple.value(), tuple)));
                }
            } else if (op_type.equals("-")) {
                aliveTuples.value().remove(tuple);
                if (aliveCount.value() == 1) {
                    out.collect(new Tuple2<>(op_type, getJoinedOrder(prevTuple.value(), tuple)));
                }
            }
        }
    }

    public order getJoinedOrder(customer c, order o) {
        o.setField("N_NAME", c.getField("N_NAME"));
        o.setField("C_CUSTKEY", c.getField("C_CUSTKEY"));
        o.setField("C_NAME", c.getField("C_NAME"));
        o.setField("C_ACCTBAL", c.getField("C_ACCTBAL"));
        o.setField("C_ADDRESS", c.getField("C_ADDRESS"));
        o.setField("C_PHONE", c.getField("C_PHONE"));
        o.setField("C_COMMENT", c.getField("C_COMMENT"));
        return o;
    }
}