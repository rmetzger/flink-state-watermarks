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
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 *
 */
public class EventCounter {

	private static final Logger LOG = LoggerFactory.getLogger(EventCounter.class);


	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		see.getConfig().setAutoWatermarkInterval(8_000L);
		see.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		see.getCheckpointConfig().setCheckpointInterval(30_000L);
		see.getConfig().setRestartStrategy(RestartStrategies.noRestart());

		if(pt.has("rocksdb")) {
			see.setStateBackend(new RocksDBStateBackend(pt.get("rocksdb")));
		}

		see.setParallelism(pt.getInt("parallelism", 1));

		Properties kProps = pt.getProperties();
		kProps.setProperty("group.id", UUID.randomUUID().toString());

		DataStream<Tuple2<Long, Long>> events = see.addSource(new OutOfOrderDataGenerator.EventGenerator(pt), "Out of order data generator").setParallelism(pt.getInt("genPar", 1));;

		events.flatMap(new ThroughputLogger<Tuple2<Long, Long>>(8, 200_000L)).setParallelism(1);


		events = events.assignTimestamps(new TSExtractor(pt));

		// do a tumbling time window: make sure every userId (key) has exactly 3 elements
		Tuple3<Long, Long, Long> initial = new Tuple3<>();
		initial.f0 = 0L;
		initial.f1= Long.MAX_VALUE;
		initial.f2 = 0L;
		DataStream<Tuple3<Long, Long, Long>> countPerUser = events.keyBy(1)
				.timeWindow(Time.minutes(1)).apply(initial, new CountingFold(), new PerKeyCheckingWindow(pt));


		// apply(R initialValue, FoldFunction<T, R> foldFunction, WindowFunction<R, R, K, W> function)

	//	DataStream<JSONObject> countPerUser = events.keyBy(new JsonKeySelector("userId")).flatMap(new CustomWindow(pt));

		// make sure for each tumbling window, we have the right number of users
		countPerUser.timeWindowAll(Time.minutes(1)).apply(0L, new AllWindowCountAllFold(), new AllWindowCheckingWindow(pt));

		see.execute("Data Generator: " + pt.getProperties());
	}

	private static class TSExtractor implements TimestampExtractor<Tuple2<Long, Long>> {
		private final long maxTimeVariance;
		private long maxTs = 0;
		public TSExtractor(ParameterTool pt) {
			this.maxTimeVariance = pt.getLong("timeSliceSize");

		}

		@Override
		public long extractTimestamp(Tuple2<Long, Long> jsonObject, long l) {
			if(jsonObject.f0 > maxTs) {
				maxTs = jsonObject.f0;
			}
			return jsonObject.f0;
		}

		@Override
		public long extractWatermark(Tuple2<Long, Long> jsonObject, long l) {
			return Long.MIN_VALUE;
		}

		@Override
		public long getCurrentWatermark() {
			long wm = maxTs - maxTimeVariance;
			return wm;
		}
	}


	/**
	 * Count key frequency and keep track of min max time
	 */
	private static class CountingFold implements FoldFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {

		@Override
		public Tuple3<Long, Long, Long> fold(Tuple3<Long, Long, Long> accumulator, Tuple2<Long, Long> value) throws Exception {
			long time = value.f0;

			accumulator.f0 = accumulator.f0 + 1;
			accumulator.f1 = Math.min(accumulator.f1, time);
			accumulator.f2 = Math.max(accumulator.f2, time);
			if(accumulator.f0 > 3 ){
				throw new RuntimeException("Count to high " + accumulator.f0);
			}
			return accumulator;
		}
	}

	private static class PerKeyCheckingWindow implements WindowFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple, TimeWindow> {
		private final long expectedFinal;
		private ParameterTool pt;

		public PerKeyCheckingWindow(ParameterTool pt) {
			this.pt = pt;
			expectedFinal = pt.getLong("eventsKerPey") * pt.getLong("eventsPerKeyPerGenerator");
		}

		@Override
		public void apply(Tuple userId, TimeWindow timeWindow, Tuple3<Long, Long, Long> finalAccu, Collector<Tuple3<Long, Long, Long>> collector) throws Exception {

		//	System.out.println("Got window for key "+userId+" with finalCount="+finalCount);

			// ensure we counted exactly 3 for the user id
			if(finalAccu.f0 != expectedFinal) {
				throw new RuntimeException("Final count is = " + finalAccu.f0 + " expected " + expectedFinal);
			}

			collector.collect(finalAccu);
		}
	}

	private static class AllWindowCountAllFold implements FoldFunction<Tuple3<Long, Long, Long>, Long> {
		@Override
		public Long fold(Long aLong, Tuple3<Long, Long, Long> o) throws Exception {
			return aLong + 1;
		}
	}

	private static class AllWindowCheckingWindow implements AllWindowFunction<Long, Long, TimeWindow> {
		private ParameterTool pt;

		public AllWindowCheckingWindow(ParameterTool pt) {
			this.pt = pt;
		}

		@Override
		public void apply(TimeWindow timeWindow, Long aLong, Collector<Long> collector) throws Exception {
			LOG.info("Got number of keys " + aLong +" for time starting at " + timeWindow.getStart());
			if(aLong != pt.getLong("numKeys")) {
				throw new RuntimeException("Number of keys is " + aLong);
			}
		}
	}

	/*private static class CustomWindow extends RichFlatMapFunction<JSONObject, JSONObject> {

		private final long maxTimeVariance;
		private final ParameterTool pt;
		private long maxTs;
		private long expectedFinal;

		public CustomWindow(ParameterTool pt) {
			this.pt = pt;
			this.maxTimeVariance = pt.getLong("timeSliceSize");
		}
		private ValueState<Map<Long, Integer>> state;
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			Map<Long, Integer> map = new HashMap<>();
			TypeInformation<Map<Long, Integer>> typeInfo = TypeExtractor.getForObject(map);
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("window", typeInfo, map));
			expectedFinal = pt.getLong("eventsKerPey") * pt.getLong("eventsPerKeyPerGenerator");
		}

		@Override
		public void flatMap(JSONObject jsonObject, Collector<JSONObject> collector) throws Exception {
		//	System.out.println("Incoming = "  + jsonObject);
		//	Thread.sleep(400);
			Long time = Long.parseLong( jsonObject.get("time").toString() );

			Map<Long, Integer> map = state.value();

			Long key = time / 60_000;
			Integer count = map.get(key);
			if (count == null) {
				count = 1;
			} else {
				count++;
			}
			map.put(key, count);

			// implement watermark handling:
			if(time > maxTs) {
				maxTs = time;
			}
			long wm =  maxTs - maxTimeVariance;

			// we are able to evaluate the window
			Iterator<Map.Entry<Long, Integer>> mapIter = map.entrySet().iterator();
			while(mapIter.hasNext()) {
				Map.Entry<Long, Integer> e = mapIter.next();
				if(e.getKey() <  (wm / 60_000)) {
					// entry is older than watermark: we can safely evaluate
					if(e.getValue() != expectedFinal) {
						throw new RuntimeException("Final count is = " + e.getValue() + " expected " + expectedFinal);
					}
					mapIter.remove(); // remove entry. has been processed.
					System.out.println("Found good window " + e.getKey() + " count " + e.getValue());
				}
			}



			// update state
			state.update(map);
		}
	} */
}
