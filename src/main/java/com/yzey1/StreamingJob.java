/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yzey1;

import java.util.Arrays;
import com.yzey1.DataTuple.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	public static final OutputTag<Tuple2<String, DataTuple>> nationTag = new OutputTag<Tuple2<String, DataTuple>>("nation"){};
	public static final OutputTag<Tuple2<String, DataTuple>> customerTag = new OutputTag<Tuple2<String, DataTuple>>("customer"){};
	public static final OutputTag<Tuple2<String, DataTuple>> ordersTag = new OutputTag<Tuple2<String, DataTuple>>("orders"){};
	public static final OutputTag<Tuple2<String, DataTuple>> lineitemTag = new OutputTag<Tuple2<String, DataTuple>>("lineitem"){};

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// set the parallelism to 1
		env.setParallelism(1);

		// read data
		String inputPath = "src/main/resources/data";
		DataStream<String> inputData = env.readTextFile(inputPath+"/ops_test.txt");

		// parse line
		SingleOutputStreamOperator<Tuple2<String, DataTuple>> inputData1 = inputData.process(new splitStream());

		// split data
		DataStream<Tuple2<String, DataTuple>> nation = inputData1.getSideOutput(nationTag);
		DataStream<Tuple2<String, DataTuple>> customer = inputData1.getSideOutput(customerTag);
		DataStream<Tuple2<String, DataTuple>> orders = inputData1.getSideOutput(ordersTag);
		DataStream<Tuple2<String, DataTuple>> lineitem = inputData1.getSideOutput(lineitemTag);

		// process nation
		DataStream<Tuple2<String, DataTuple>> processedNation = nation
				.keyBy(t -> t.f1.fk_value)
				.process(new NationProcessFunction());
		DataStream<Tuple2<String, DataTuple>> processedCustomer = processedNation.connect(customer)
				.keyBy(t -> t.f1.pk_value, t -> t.f1.fk_value)
				.process(new CustomerProcessFunction());
		DataStream<Tuple2<String, DataTuple>> processedOrder = processedCustomer.connect(orders)
				.keyBy(t -> t.f1.pk_value, t -> t.f1.fk_value)
				.process(new OrdersProcessFunction());
		DataStream<Tuple2<String, DataTuple>> processedLineitem = processedOrder.connect(lineitem)
				.keyBy(t -> t.f1.pk_value, t -> t.f1.fk_value)
				.process(new LineitemProcessFunction());

		// aggregate the results
		DataStream<Double> result = processedLineitem.keyBy(t -> t.f1.pk_value)
				.process(new AggregationProcessFunction());

		// print the result each time
//		inputData1.print();
//		processedCustomer.print();
		result.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");

	}

	private static class splitStream extends ProcessFunction<String, Tuple2<String, DataTuple>> {
		@Override
		public void processElement(String value, Context ctx, Collector<Tuple2<String, DataTuple>> out) throws Exception {
			String[] split_line = value.split("\\|");
			String op = split_line[0];
			String table = split_line[1];
			Object[] data = Arrays.copyOfRange(split_line, 2, split_line.length);
			DataTuple dt;
			OutputTag<Tuple2<String, DataTuple>> outputTag;
			switch (table) {
				case "orders":
					dt = new order(data);
					outputTag = ordersTag;
					break;
				case "customer":
					dt = new customer(data);
					outputTag = customerTag;
					break;
				case "nation":
					dt = new nation(data);
					outputTag = nationTag;
					break;
				case "lineitem":
					dt = new lineitem(data);
					outputTag = lineitemTag;
					break;
				default:
					return;
			}
			System.out.println("Processing: " + op + " " + table + " " + dt);
			ctx.output(outputTag, new Tuple2<>(op, dt));
		}
	}
}
