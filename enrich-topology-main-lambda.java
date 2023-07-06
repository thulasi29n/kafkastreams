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
---

public class TopologyBuilder {
    public static Topology buildTopology(StreamsBuilder builder, String inputTopic, String outputTopic) {
        // Define the topology
        KStream<String, Transaction> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), AvroSerdes.getTransactionAvroSerde()));
        
        KStream<String, EnrichedTransaction> enrichedStream = inputStream.transform(Enricher::new);

        enrichedStream.to(outputTopic, Produced.with(Serdes.String(), AvroSerdes.getEnrichedTransactionAvroSerde()));

        // Build and return the topology
        return builder.build();
    }
}


---------

import org.apache.kafka.streams.*;
import java.util.Properties;

public class StateStoreEnrichmentExample {
    public static void main(String[] args) {
        Properties config = KafkaConfig.getProperties();
        String inputTopic = KafkaConfig.getInputTopicName(config);
        String outputTopic = KafkaConfig.getOutputTopicName(config);

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = TopologyBuilder.buildTopology(builder, inputTopic, outputTopic);

        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
