package com.example.demo;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;


@ExtendWith(SpringExtension.class)

@SpringBootTest(classes={com.example.demo.DemoApplication.class})


public class KafkaStreamsTest {
    @Autowired
    MyConfig myConfig;
    @Autowired
    WordCountProcessor wordCountProcessor;

    @Test
    public void givenInputMessages_whenProcessed_thenWordCountIsProduced() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        wordCountProcessor.buildPipeline(streamsBuilder);
        Topology topology = streamsBuilder.build();

        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, myConfig.getProperties())) {
            TestInputTopic<String, String> inputTopic = topologyTestDriver
                    .createInputTopic("input-topic", new StringSerializer(), new StringSerializer());

            TestOutputTopic<String, Long> outputTopic = topologyTestDriver
                    .createOutputTopic("output-topic", new StringDeserializer(), new LongDeserializer());

            inputTopic.pipeInput("key", "hello world");
            inputTopic.pipeInput("key2", "hello");

            List output = outputTopic.readKeyValuesToList();
            Assertions.assertThat(output)
                    .containsExactly(
                            KeyValue.pair("hello", 1L),
                            KeyValue.pair("world", 1L),
                            KeyValue.pair("hello", 2L)
                    );
        }
    }
}