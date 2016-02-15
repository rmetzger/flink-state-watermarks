package com.dataartisans;

/**
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

import com.dataartisans.utils.ThroughputLogger;
import net.minidev.json.JSONObject;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.XORShiftRandom;

import java.util.Random;

/**
 *
 */
public class OutOfOrderDataGenerator {

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);

		DataStream<String> events = see.addSource(new EventGenerator(pt), "Out of order data generator")
				.setParallelism(pt.getInt("eventsKerPey"));

	//	events.print().setParallelism(1);
		events.flatMap(new ThroughputLogger<String>(32, 100_000L));
	//	events.addSink(new FlinkKafkaProducer08<>(pt.getRequired("topic"), new SimpleStringSchema(), pt.getProperties()));

		see.execute("Data Generator: " + pt.getProperties());
	}

	/**
	 * Generate E events per key
	 */
	private static class EventGenerator extends RichParallelSourceFunction<String> {
		private final ParameterTool pt;
		private boolean running = true;
		private final long numKeys;
		private final long eventsPerKey;
	//	private final int timeVariance; // the max delay of the events
		private final long timeSliceSize;
		private  Random rnd;

		public EventGenerator(ParameterTool pt) {
			this.pt = pt;
			this.numKeys = pt.getLong("numKeys");
			this.eventsPerKey = pt.getLong("eventsPerKeyPerGenerator", 1);
			//this.timeVariance = pt.getInt("timeVariance", 10_000); // 10 seconds
			this.timeSliceSize = pt.getLong("timeSliceSize"); // 1 minute
		}

		@Override
		public void run(SourceContext<String> sourceContext) throws Exception {
			rnd = new XORShiftRandom(getRuntimeContext().getIndexOfThisSubtask());
			long time = 0;
			while(running) {
				for(long key = 0; key < numKeys; key++) {
					for(long eventPerKey = 0; eventPerKey < eventsPerKey; eventPerKey++) {
						final JSONObject out = new JSONObject();
					//	int tVar = rnd.nextInt(this.timeVariance);
						out.put("time", time + rnd.nextInt((int)timeSliceSize)); // distribute events within slice size
						out.put("userId", key);
						String o = out.toJSONString();
						System.out.println( o);
						sourceContext.collect(o);
						Thread.sleep(1000);
					}
				}
				// advance base time
				time += timeSliceSize;
			}
			sourceContext.close();
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
