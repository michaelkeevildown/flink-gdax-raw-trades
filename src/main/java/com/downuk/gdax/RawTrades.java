package com.downuk.gdax;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class RawTrades
{
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // setup kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");
        FlinkKafkaConsumer08<ObjectNode> kafkaSource = new FlinkKafkaConsumer08<>(
                "mytopic",
                new JSONKeyValueDeserializationSchema(false),
                properties
        );
        kafkaSource.setStartFromEarliest();

        // consume date from kafka and apply watermarks
        DataStream<ObjectNode> stream = env
                .addSource(kafkaSource)
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        // print
        stream.print();

        /*
            Elasticsearch Configuration
        */
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<ObjectNode> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<ObjectNode>() {
                    private IndexRequest createIndexRequest(ObjectNode trade) {

                        // remove the value node so the fields are at the base of the json payload
                        JsonNode jsonOutput = trade.get("value");

                        return Requests.indexRequest()
                                .index("raw-trades")
                                .type("trade")
                                .source(jsonOutput.toString(), XContentType.JSON);
                    }

                    @Override
                    public void process(ObjectNode trade, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(trade));
                    }
                }
        );

        // set number of events to be seen before writing to Elasticsearch
        esSinkBuilder.setBulkFlushMaxActions(1);

        // finally, build and add the sink to the job's pipeline
        stream.addSink(esSinkBuilder.build());

        // run job
        env.execute("Load raw trades");
    }
}
