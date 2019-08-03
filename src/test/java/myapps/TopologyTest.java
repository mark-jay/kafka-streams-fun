package myapps;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class TopologyTest {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    @Test
    public void testStreams() {

        Topology topology = createTopology();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, config)) {

            ConsumerRecordFactory<String, Long> factory = new ConsumerRecordFactory<>(
                    INPUT_TOPIC, new StringSerializer(), new LongSerializer());

            testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", 1l));
            testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", 2l));
            testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", 3l));

            ProducerRecord<String, String> pr1 = testDriver.readOutput("output-topic", new StringDeserializer(), new StringDeserializer());
            ProducerRecord<String, String> pr2 = testDriver.readOutput("output-topic", new StringDeserializer(), new StringDeserializer());
            ProducerRecord<String, String> pr3 = testDriver.readOutput("output-topic", new StringDeserializer(), new StringDeserializer());

            Assert.assertEquals("1,1", pr1.value());
            Assert.assertEquals("2,3", pr2.value());
            Assert.assertEquals("3,6", pr3.value());
        }
    }

    private Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Long> stream = builder.stream(INPUT_TOPIC);

        KTable<String, Long> table = stream.groupByKey().aggregate(
                () -> 0L,
                (s, value, aggregate) -> value + aggregate,
                Materialized.as("store")
        );

        KStream<String, String> joined = stream
                .join(table, (value, aggregate) -> value + "," + aggregate);

        joined.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

}