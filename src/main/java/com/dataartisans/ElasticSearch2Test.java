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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ElasticSearch2Test {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticSearch2Test.class);


	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);
		// set up the execution environment
		final StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);

		see.setParallelism(pt.getInt("parallelism", 1));


		DataStream<Tuple2<Long, Long>> events = see.addSource(new OutOfOrderDataGenerator.EventGenerator(pt), "Out of order data generator").setParallelism(pt.getInt("genPar", 1));

		events.flatMap(new ThroughputLogger<Tuple2<Long, Long>>(8, 10_000L)).setParallelism(1);

		Map<String, String> config = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");
		config.put("cluster.name", "elasticsearch");

		// need this with ElasticSearch v2.x
		//config.put("path.home", "/home/robert/flink-workdir/elasticsearch/elasticsearch-2.2.1");
		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress("localhost", 9300));

		events.addSink(new ElasticsearchSink<>(config, transports, new ElasticsearchSinkFunction<Tuple2<Long, Long>>() {
			public IndexRequest createIndexRequest(Tuple2<Long, Long> element, RuntimeContext ctx) {
				Map<String, Object> json = new HashMap<>();
				json.put("time", element.f0);
				json.put("key", element.f1);

				return Requests.indexRequest()
						.index("my-index")
						.type("my-type")
						.source(json);
			}

			@Override
			public void process(Tuple2<Long, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
				indexer.add(createIndexRequest(element, ctx));
			}
		}));

		see.execute("ES Test: " + pt.getProperties());
	}


}
