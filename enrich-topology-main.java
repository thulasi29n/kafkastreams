public class Enricher implements Transformer<String, Transaction, KeyValue<String, EnrichedTransaction>> {
    private KeyValueStore<String, GenericRecord> stateStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore<String, GenericRecord>) context.getStateStore("my-state-store");
    }

    @Override
    public KeyValue<String, EnrichedTransaction> transform(String key, Transaction inputTransaction) {
        // Perform the lookup operation
        GenericRecord stateRecord = stateStore.get(key);

        if (stateRecord != null) {
            // Extract the required attributes from the state record
            String name = stateRecord.get("name").toString();
            String value = stateRecord.get("value").toString();
            String country = stateRecord.get("country").toString();

            // Create an enriched transaction object
            EnrichedTransaction enrichedTransaction = new EnrichedTransaction(
                    inputTransaction.getId(),
                    inputTransaction.getAmount(),
                    inputTransaction.getNumber(),
                    name,
                    value,
                    country
            );

            // Return the enriched transaction as the output record
            return KeyValue.pair(key, enrichedTransaction);
        }

        // Return null if no enrichment is performed for the input record
        return null;
    }

    @Override
    public void close() {
        // Close any resources if needed
    }
}


import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class TopologyBuilder {
    public static Topology buildTopology(StreamsBuilder builder) {
        // Define a persistent state store
        StoreBuilder<KeyValueStore<String, GenericRecord>> storeBuilder = Stores.persistentKeyValueStore("my-state-store");
        builder.addStateStore(storeBuilder);

        // Process the input stream
        KStream<String, Transaction> inputStream = builder.stream("input-topic", Consumed.with(Serdes.String(), AvroSerdes.getTransactionAvroSerde()));

        KStream<String, EnrichedTransaction> enrichedStream = inputStream.transform(new TransformerSupplier<String, Transaction, KeyValue<String, EnrichedTransaction>>() {
            @Override
            public Transformer<String, Transaction, KeyValue<String, EnrichedTransaction>> get() {
                return new Enricher();
            }
        });

        enrichedStream.to("output-topic", Produced.with(Serdes.String(), AvroSerdes.getEnrichedTransactionAvroSerde()));

        // Build and return the topology
        return builder.build();
    }
}


import org.apache.kafka.streams.*;
import java.util.Properties;

public class StateStoreEnrichmentExample {
    public static void main(String[] args) {
        Properties config = KafkaConfig.getProperties();

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = TopologyBuilder.buildTopology(builder);

        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
