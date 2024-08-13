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
import java.util.concurrent.TimeUnit;

import com.yzey1.DataTuple.*;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.core.fs.Path;


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

		Runtime runtime = Runtime.getRuntime();
		// set up the streaming execution environment
		// Record the start time
		long initialMemory = runtime.totalMemory() - runtime.freeMemory();
		long startTime = System.currentTimeMillis();

		// set up the streaming execution environment
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Configuration configuration = new Configuration();
		configuration.setString("rest.port","8081");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

		// set the parallelism to 1
		env.setParallelism(1);

		// read data
		String sf = "0.1";
//		String input_type = "init";
		String input_type = "update";
		String inputPath = "tpch_datasets/data_sf"+sf+"/";
		String filename = "ops_sf"+sf+"_"+input_type+"_1.0.txt";

		String outputPath = "output";
		String outputFilename = filename.replace(".txt", ".csv");

		// data source
		DataStreamSource<String> inputData = env.readTextFile(inputPath+"/"+ filename);

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
		DataStream<Tuple9<String, String, Double, String, String, String, String, Double, Double>> result = processedLineitem
				.keyBy(t -> t.f1.getField("C_CUSTKEY").toString())
				.process(new AggregationProcessFunction());

		// print the results
//		processedLineitem.map(t -> t.f1.getField("output_fileds").toString()).print();

		// write the results to a file

		StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
				.withRollingPolicy(DefaultRollingPolicy.builder()
						.withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
						.withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
						.build())
				.build();

		result.map(StreamingJob::convertTupleToString).addSink(sink);

//		result.writeAsCsv(outputPath + "/" + outputFilename, FileSystem.WriteMode.OVERWRITE, "\n", "|");

		// execute program
		env.execute("Flink Streaming Java API Skeleton");

		// Capture end time and final memory usage
		long endTime = System.currentTimeMillis();
		long finalMemory = runtime.totalMemory() - runtime.freeMemory();

		// Calculate the runtime and memory consumption
		long runtimeDuration = endTime - startTime;
		long memoryConsumption = finalMemory - initialMemory;

		// Print the results
        System.out.println("Runtime: " + runtimeDuration + " ms");
		System.out.println("Memory consumption: " + (memoryConsumption / (1024 * 1024)) + " MB");

	}


	private static class splitStream extends ProcessFunction<String, Tuple2<String, DataTuple>> {
		@Override
		public void processElement(String value, Context ctx, Collector<Tuple2<String, DataTuple>> out) {
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
//			System.out.println("Processing: " + op + " " + table + " " + dt.pk_value);
			ctx.output(outputTag, new Tuple2<>(op, dt));
		}
	}

	public static String convertTupleToString(Tuple9<String, String, Double, String, String, String, String, Double, Double> tuple) {
		// handle with null values
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < tuple.getArity(); i++) {
			if (tuple.getField(i) == null) {
				str.append("null");
			} else {
				str.append(tuple.getField(i).toString());
			}
			if (i < tuple.getArity() - 1) {
				str.append("|");
			}
		}
		return str.toString();
	}
}
