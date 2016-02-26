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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 *
 */
public class OutOfOrderDataGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(OutOfOrderDataGenerator.class);

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);

		see.setParallelism(pt.getInt("eventsKerPey"));
		DataStream<Tuple2<Long, Long>> events = see.addSource(new EventGenerator(pt), "Out of order data generator");

	//	events.print().setParallelism(1);
	//	events.flatMap(new ThroughputLogger<String>(32, 100_000L));
	//	events.addSink(new FlinkKafkaProducer08<>(pt.getRequired("topic"), new SimpleStringSchema(), pt.getProperties()));

		see.execute("Data Generator: " + pt.getProperties());
	}

	/**
	 * Generate E events per key
	 */
	public static class EventGenerator extends RichParallelSourceFunction<Tuple2<Long, Long>> implements Checkpointed<Tuple2<Long,Long>> {
		private final ParameterTool pt;

		//checkpointed
		private Long time = 0L;
		private Long key = 0L;

		private volatile boolean running = true;
		private final long numKeys;
		private final long eventsPerKey;
	//	private final int timeVariance; // the max delay of the events
		private final long timeSliceSize;
		private Random rnd;

		public EventGenerator(ParameterTool pt) {
			this.pt = pt;
			this.numKeys = pt.getLong("numKeys");
			this.eventsPerKey = pt.getLong("eventsPerKeyPerGenerator", 1);
			//this.timeVariance = pt.getInt("timeVariance", 10_000); // 10 seconds
			this.timeSliceSize = pt.getLong("timeSliceSize"); // 1 minute
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
			rnd = new XORShiftRandom(getRuntimeContext().getIndexOfThisSubtask());

			while(running) {
				while(true) {
				//for (key = 0L; key < numKeys; key++) {
					synchronized (sourceContext.getCheckpointLock()) {
						for (long eventPerKey = 0; eventPerKey < eventsPerKey; eventPerKey++) {
							final Tuple2<Long, Long> out = new Tuple2<>();
							out.f0 = time + rnd.nextInt((int) timeSliceSize); // distribute events within slice size
							out.f1 = key;
				//			 System.out.println("Outputting key " + key + " for time " + time);
							sourceContext.collect(out);
							if (!running) {
								return; // we are done
							}
						}
					}
					if(++key >= numKeys) {
						key = 0L;
						break;
					}
				}
				// advance base time
				time += timeSliceSize;
			}
			sourceContext.close();
		}

		@Override
		public void cancel() {
			LOG.info("Received cancel in EventGenerator");
			running = false;
		}

		@Override
		public Tuple2<Long, Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return new Tuple2<>(this.time, this.key);
		}

		@Override
		public void restoreState(Tuple2<Long, Long> state) throws Exception {
			this.time = state.f0;
			this.key = state.f1;
		}
	}
}
