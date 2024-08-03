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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

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

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//
		// set the parallelism to 1
		env.setParallelism(1);

		// read data
		String inputPath = "src/main/resources/data";
		DataStream<String> inputData = env.readTextFile(inputPath+"/ops_init.txt");

		// parse data
		StreamingJob sj = new StreamingJob();
		SingleOutputStreamOperator<Tuple2<String, Object[]>> parsedData = sj.parseData(inputData);

		// group by key
		KeyedStream<Tuple2<String, Object[]>, String> groupedData = parsedData.keyBy(t -> t.f0);

		// process data
		SingleOutputStreamOperator<Tuple2<String, Object[]>> processedData = groupedData.map(new MapFunction<Tuple2<String, Object[]>, Tuple2<String, Object[]>>() {
			@Override
			public Tuple2<String, Object[]> map(Tuple2<String, Object[]> value) throws Exception {
				String key = value.f0;
				Object[] data = value.f1;

				String op = key.substring(0, 1);
				String table = key.substring(1);

				Tuple2<String, Object[]> result = null;

				// process data based on operation and table
				switch (table) {
					case "nation":
						// process data for Nation
						result = NationProcessFunction.process(data);
						// print processing class
						System.out.println("Running NationProcessFunction class.");
					case "customer":
						// process data for Customer
						result = CustomerProcessFunction.process(data);
						// print processing class
						System.out.println("Running CustomerProcessFunction class.");
					case "orders":
						// process data for Orders
						result = OrdersProcessFunction.process(data);
						// print processing class
						System.out.println("Running OrdersProcessFunction class.");
					case "lineitem":
						// process data for Lineitem
						result = LineitemProcessFunction.process(data);
						// print processing class
						System.out.println("Running LineitemProcessFunction class.");
//					default:
						// default processing
//						result = new Tuple2<>(key, data);
				}

				return result;
			}
		});


		// print the result each time
//		SinkFunction<Tuple2<String, Object[]>> printSink = new PrintSinkFunction<>();
//		groupedData.addSink(printSink);
		// print the key of group data
		SinkFunction<Tuple2<String, Object[]>> printSink = new PrintSinkFunction<>();
		processedData.addSink(printSink);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");

	}

	private SingleOutputStreamOperator<Tuple2<String, Object[]>> parseData(DataStream<String> inputData) {
        return inputData.map(new MapFunction<String, Tuple2<String, Object[]>>() {
			@Override
			public Tuple2<String, Object[]> map(String s) throws Exception {
				String[] split_line = s.split(",");
				String op_table = split_line[0]+split_line[1];
				Object[] data = split_line[2].split("\\|");
				// return op_table is the key, and data is the value
				return new Tuple2<>(op_table, data);
			}
		});
	}


}
