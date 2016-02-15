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
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
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

import java.util.Properties;
import java.util.UUID;

/**
 *
 */
public class EventCounter {


	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromPropertiesFile(args[0]);
		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		see.getConfig().setAutoWatermarkInterval(8_000L);
		see.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		see.getCheckpointConfig().setCheckpointInterval(30_000L);
		see.setNumberOfExecutionRetries(pt.getInt("executionRetires", 0));

		Properties kProps = pt.getProperties();
		kProps.setProperty("group.id", UUID.randomUUID().toString());
		DataStream<String> eventsAsStrings = see.addSource(new FlinkKafkaConsumer08<>(pt.getRequired("topic"), new SimpleStringSchema(), kProps));
		eventsAsStrings.flatMap(new ThroughputLogger<String>(32, 100_000L));
		DataStream<JSONObject> events = eventsAsStrings.map(new ParseJson());

		events.assignTimestamps(new TSExtractor(pt));

		// do a tumbling time window: make sure every userId (key) has exactly 3 elements
		JSONObject initial = new JSONObject();
		initial.put("count", 0L);
		initial.put("firstTime", Long.MAX_VALUE);
		initial.put("lastTime", 0L);
		DataStream<JSONObject> countPerUser = events.keyBy(new JsonKeySelector("userId"))
				.timeWindow(Time.minutes(1)).apply(initial, new CountingFold(), new PerKeyCheckingWindow(pt));

		// make sure for each tumbling window, we have the right number of users
		countPerUser.timeWindowAll(Time.minutes(1)).apply(0L, new AllWindowCountAllFold(), new AllWindowCheckingWindow(pt));

		see.execute("Data Generator: " + pt.getProperties());
	}

	private static class ParseJson implements MapFunction<String, JSONObject> {
		private transient JSONParser parser;

		@Override
		public JSONObject map(String s) throws Exception {
			if(parser == null) {
				parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
			}
			return (JSONObject) parser.parse(s);
		}
	}

	private static class TSExtractor implements TimestampExtractor<JSONObject> {
		private final long maxTimeVariance;
		private long maxTs = 0;
		public TSExtractor(ParameterTool pt) {
			this.maxTimeVariance = pt.getLong("timeSliceSize");

		}

		@Override
		public long extractTimestamp(JSONObject jsonObject, long l) {
			long ts = (long) jsonObject.get("time");
			if(ts > maxTs) {
				maxTs = ts;
			}
			return ts;
		}

		@Override
		public long extractWatermark(JSONObject jsonObject, long l) {
			return Long.MIN_VALUE;
		}

		@Override
		public long getCurrentWatermark() {
			System.out.println("emitting watermark");
			return maxTs - maxTimeVariance;
		}
	}

	private static class JsonKeySelector implements KeySelector<JSONObject, Object> {
		private final String key;

		public JsonKeySelector(String key) {
			this.key = key;
		}

		@Override
		public Object getKey(JSONObject jsonObject) throws Exception {
			return jsonObject.get(key);
		}
	}

	/**
	 * Count key frequency and keep track of min max time
	 */
	private static class CountingFold implements FoldFunction<JSONObject, JSONObject> {

		@Override
		public JSONObject fold(JSONObject accumulator, JSONObject value) throws Exception {
			long cnt = (long)accumulator.get("count");
			long minAccu = (long)accumulator.get("firstTime");
			long maxAccu = (long)accumulator.get("lastTime");

			long time = (long)value.get("time");

			accumulator.put("count", cnt + 1);
			accumulator.put("firstTime", Math.min(minAccu, time));
			accumulator.put("lastTime", Math.max(maxAccu, time));
			return accumulator;
		}
	}

	private static class PerKeyCheckingWindow implements WindowFunction<JSONObject, JSONObject, Object, TimeWindow> {
		private ParameterTool pt;

		public PerKeyCheckingWindow(ParameterTool pt) {
			this.pt = pt;
		}

		@Override
		public void apply(Object userId, TimeWindow timeWindow, JSONObject finalAccu, Collector<JSONObject> collector) throws Exception {
			long finalCount = (long) finalAccu.get("count");
			long minVal = (long)finalAccu.get("firstTime");
			long maxVal = (long)finalAccu.get("lastTime");
			System.out.println("Got window for key "+userId+" with finalCount="+finalCount+" min="+minVal+" max="+maxVal);

			// ensure we counted exactly 3 for the user id
			if(finalCount != pt.getLong("eventsKerPey")) {
				throw new RuntimeException("Final count is = " + finalCount);
			}
			if(timeWindow.getStart() != minVal) {
				throw new RuntimeException("Start time wrong " + timeWindow.getStart() + ", " + minVal);
			}

			if(timeWindow.getEnd() != maxVal) {
				throw new RuntimeException("Start time wrong " + timeWindow.getEnd() + ", " + maxVal);
			}
			collector.collect(finalAccu);
		}
	}

	private static class AllWindowCountAllFold implements FoldFunction<JSONObject, Long> {
		@Override
		public Long fold(Long aLong, JSONObject o) throws Exception {
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
			System.out.println("Got number of keys " + aLong +" for time starting at " + timeWindow.getStart());
			if(aLong != pt.getLong("numKeys")) {
				throw new RuntimeException("Number of keys is " + aLong);
			}
		}
	}
}
