package myapps;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
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

            testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", 1L));
            testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", 2L));
            testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", 3L));

            ProducerRecord<String, String> pr1 = testDriver.readOutput(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
            ProducerRecord<String, String> pr2 = testDriver.readOutput(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());
            ProducerRecord<String, String> pr3 = testDriver.readOutput(OUTPUT_TOPIC, new StringDeserializer(), new StringDeserializer());

            Assert.assertEquals("1,1", pr1.value());
            Assert.assertEquals("2,3", pr2.value());
            Assert.assertEquals("3,6", pr3.value());
        }
    }

    private Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Long> inputStream = builder.stream(INPUT_TOPIC);

        KTable<String, Long> table = inputStream.groupByKey().aggregate(
                () -> 0L,
                (key, value, aggregate) -> value + aggregate,
                Materialized.as("store")
        );

        KStream<String, String> joined = inputStream
                .join(table, (value, aggregate) -> value + "," + aggregate);

        joined.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

}