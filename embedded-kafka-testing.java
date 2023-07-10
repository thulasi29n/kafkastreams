import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class WorkflowTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, TransactionAvro> inputTopic;
    private TestOutputTopic<String, TransactionAvro> foundTopic;
    private TestOutputTopic<String, TransactionAvro> notFoundTopic;
    private KeyValueStore<String, EnrichmentAvro> stateStore;

    @Before
    public void setup() {
        // Set up the embedded Kafka cluster
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TransactionAvroSerde.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, TransactionAvro> inputStream = builder.stream("input-topic");

        // Repartition the input topic using selectKey()
        KStream<String, TransactionAvro> repartitionedStream = inputStream.selectKey((key, value) -> /* extract the desired key */);

        // Create a mock state store
        stateStore = testDriver.getKeyValueStore("state-store");
        stateStore.put("key1", new EnrichmentAvro(/* set your mock data for lookup result */));
        stateStore.put("key2", new EnrichmentAvro(/* set your mock data for lookup result */));

        // Perform enrichment logic using the mock state store
        KStream<String, TransactionAvro> enrichedStream = repartitionedStream
                .transformValues(() -> new EnrichmentTransformer("state-store"), "state-store")
                .branch(
                        (key, value) -> value.isLookupFound(),   // Found records
                        (key, value) -> true                     // Not found records
                )[0].mapValues(value -> enrichTransaction(value));
        
        // Write found records to found topic
        enrichedStream.filter((key, value) -> value.isLookupFound())
                .to("found-topic", Produced.with(Serdes.String(), new TransactionAvroSerde()));
        
        // Write not found records to not found topic
        enrichedStream.filter((key, value) -> !value.isLookupFound())
                .to("not-found-topic", Produced.with(Serdes.String(), new TransactionAvroSerde()));

        // Build the topology and create the test driver
        Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        // Create test input and output topics
        inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(), new TransactionAvroSerializer());
        foundTopic = testDriver.createOutputTopic("found-topic", new StringDeserializer(), new TransactionAvroDeserializer());
        notFoundTopic = testDriver.createOutputTopic("not-found-topic", new StringDeserializer(), new TransactionAvroDeserializer());
    }

    @After
    public void cleanup() {
        // Close the test driver
        testDriver.close();
    }

    @Test
    public void testWorkflow() {
        // Send test data to the input topic
        inputTopic.pipeInput("key1", new TransactionAvro(/* set your test data for TransactionAvro */));
        inputTopic.pipeInput("key2", new TransactionAvro(/* set your test data for TransactionAvro */));
        inputTopic.pipeInput("key3", new TransactionAvro(/* set your test data for TransactionAvro */));

        // Consume and validate output records from the found topic
        ConsumerRecord<String, TransactionAvro> foundRecord1 = foundTopic.readRecord();
        // Validate the found record

        ConsumerRecord<String, TransactionAvro> foundRecord2 = foundTopic.readRecord();
        // Validate the found record

        // Consume and validate output records from the not found topic
        ConsumerRecord<String, TransactionAvro> notFoundRecord1 = notFoundTopic.readRecord();
        // Validate the not found record

        ConsumerRecord<String, TransactionAvro> notFoundRecord2 = notFoundTopic.readRecord();
        // Validate the not found record
    }

    private TransactionAvro enrichTransaction(TransactionAvro transaction) {
        // Perform your enrichment logic here
        // Modify the transaction or enrich it based on your requirements
        // Return the enriched transaction or the original transaction if not found

        return transaction; // Modify the return statement according to your enrichment logic
    }
}
