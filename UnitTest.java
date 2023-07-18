import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class MyTopologyTest {
    private TopologyTestDriver testDriver;
    private KeyValueStore<String, InstrumentAvro> stateStore;

    @Before
    public void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TransactionAvroSerde.class);

        // Create the topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read from the input topic
        KStream<String, TransactionAvro> inputStream = builder.stream("input-topic");

        // Transform the key using selectKey()
        KStream<String, TransactionAvro> transformedStream = inputStream.selectKey((key, value) -> value.getTxnref().toString());

        // Perform state lookup and forward to appropriate topics
        transformedStream.transform(() -> new StateLookupTransformer("your-state-store-name"))
                .to("output-topic-found");
        transformedStream.filter((key, value) -> !value.isFound())
                .to("output-topic-not-found");

        // Get the state store
        Topology topology = builder.build(props);
        testDriver = new TopologyTestDriver(topology, props);
        stateStore = testDriver.getKeyValueStore("your-state-store-name");

        // Populate the state store directly
        stateStore.put("key1", new InstrumentAvro(/* avro data for key1 */));
        stateStore.put("key2", new InstrumentAvro(/* avro data for key2 */));
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testTopology() {
        // Create a factory for producing input records
        ConsumerRecordFactory<String, TransactionAvro> recordFactory =
                new ConsumerRecordFactory<>(new StringSerializer(), new TransactionAvroSerde().serializer());

        // Send an input record to the first topic
        testDriver.pipeInput(recordFactory.create("input-topic", null, new TransactionAvro("txnref1", /* avro data */)));

        // Verify the output record for found records
        OutputVerifier.compareKeyValue(
                testDriver.readOutput("output-topic-found", new StringDeserializer(), new TransactionAvroSerde().deserializer()),
                "txnref1", new TransactionAvro("txnref1", /* avro data with setFound(true) */));

        // Verify the output record for not found records
        OutputVerifier.compareKeyValue(
                testDriver.readOutput("output-topic-not-found", new StringDeserializer(), new TransactionAvroSerde().deserializer()),
                "txnref1", new TransactionAvro("txnref1", /* avro data with setFound(false) */));
    }

    public static class StateLookupTransformer extends AbstractProcessor<String, TransactionAvro> {
        private final String stateStoreName;
        private KeyValueStore<String, InstrumentAvro> stateStore;

        public StateLookupTransformer(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            stateStore = (KeyValueStore<String, InstrumentAvro>) context.getStateStore(stateStoreName);
        }

        @Override
        public void process(String key, TransactionAvro value) {
            // Perform the state lookup
            InstrumentAvro instrument = stateStore.get(key);

            // Update the TransactionAvro object based on the lookup result
            if (instrument != null) {
                value.setFound(true);
            } else {
                value.setFound(false);
            }

            // Forward the updated TransactionAvro object downstream
            context().forward(key, value);
        }
    }
}
