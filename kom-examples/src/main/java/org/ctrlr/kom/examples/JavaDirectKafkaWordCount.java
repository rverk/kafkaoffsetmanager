/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ctrlr.kom.examples;

import com.google.common.collect.Lists;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.ctrlr.kom.core.KafkaOffsetManager;
import org.ctrlr.kom.dao.IOffsetDao;
import org.ctrlr.kom.daoimplementation.Hbase1OffsetStore;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 * <brokers> is a list of one or more Kafka brokers
 * <topics> is a list of one or more kafka topics to consume from
 * <p/>
 * Example:
 * $ bin/run-example streaming.KafkaWordCount broker1-host:port,broker2-host:port topic1,topic2
 */

public final class JavaDirectKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: DirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];


        /** Setup Hbase configuration. 1.4 will support SPARK-6918 */
        final Configuration hbaseConfiguration = HBaseConfiguration.create();

        /** Keep track of offsets*/
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        final IOffsetDao dao = new Hbase1OffsetStore.Builder()
                .setHbaseConfiguration(hbaseConfiguration)
                .setOffsetTable("kafkaoffsettable").build();

        final KafkaOffsetManager osm = new KafkaOffsetManager.Builder()
                .setOffsetManager(dao)
                .setKafkaBrokerList("localhost:9092")
                .setGroupID("testGroupID")
                .setTopic("kafkaTestTopic").build();

        /** Get offsets or start at beginning. getLatestOffsets is also an option. */
        Map<TopicAndPartition, Long> offsetMap = osm.getOffsets();
        if (offsetMap.isEmpty()) {offsetMap = osm.getEarliestOffsets();}


        // Create context with 2 second batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);


        /** Creat direct kafka stream using the acquired offsets */
        JavaInputDStream<byte[]> messages = KafkaUtils.createDirectStream(
                jssc,
                byte[].class, byte[].class,
                DefaultDecoder.class, DefaultDecoder.class,
                byte[].class,
                kafkaParams,
                offsetMap,
                new Function<MessageAndMetadata<byte[], byte[]>, byte[]>() {
                    @Override
                    public byte[] call(MessageAndMetadata<byte[], byte[]> messageAndMetadata) throws Exception {
                        return messageAndMetadata.message();
                    }
                }
        );

        /** Get kafka offsets by a transform, this needs to go first else (HassOffsetRanges) will not work */
        JavaDStream<byte[]> lines = messages.transform(new Function<JavaRDD<byte[]>, JavaRDD<byte[]>>() {
            @Override
            public JavaRDD<byte[]> call(JavaRDD<byte[]> javaRDD) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
                offsetRanges.set(offsets);

                return javaRDD;
            }
        });


        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<byte[], String>() {
            @Override
            public Iterable<String> call(byte[] x) {
                return Lists.newArrayList(SPACE.split(new String(x)));
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });


        /** Write offsets to OffsetManager */
        lines.foreachRDD(new Function<JavaRDD<byte[]>, Void>() {
                             @Override
                             public Void call(JavaRDD<byte[]> javaRDD) throws Exception {

                                 for (OffsetRange o : offsetRanges.get()) {
                                     if (o.fromOffset() < o.untilOffset()) {

                                         Map<TopicAndPartition, Long> offsets = new HashMap<>();
                                         offsets.put(new TopicAndPartition(o.topic(), o.partition()), o.untilOffset());

                                         osm.setOffsets(offsets);
                                     }
                                 }
                                 return null;
                             }
                         }

        );

        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
