import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.Properties;

public class AvroSerdeConfig {

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    public static SpecificAvroSerde<Transaction> getTransactionAvroSerde(Properties properties) {
        // Configure the Serde with the externalized Schema Registry URL
        String schemaRegistryUrl = properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        SpecificAvroSerde<Transaction> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(
                ImmutableMap.of(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
                ),
                false
        );

        return avroSerde;
    }

    public static SpecificAvroSerde<EnrichedTransaction> getEnrichedTransactionAvroSerde(Properties properties) {
        // Configure the Serde with the externalized Schema Registry URL
        String schemaRegistryUrl = properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
        SpecificAvroSerde<EnrichedTransaction> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(
                ImmutableMap.of(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
                ),
                false
        );

        return avroSerde;
    }
}

public class TopologyBuilder {
    public static Topology buildTopology(StreamsBuilder builder, String inputTopic, String outputTopic) {
        // Load the properties file
        Properties properties = loadProperties();

        // Retrieve the Schema Registry URL from the properties
        String schemaRegistryUrl = properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG);

        // Configure the Avro Serdes
        SpecificAvroSerde<Transaction> transactionAvroSerde = AvroSerdeConfig.getTransactionAvroSerde(properties);
        SpecificAvroSerde<EnrichedTransaction> enrichedTransactionAvroSerde = AvroSerdeConfig.getEnrichedTransactionAvroSerde(properties);

        // Define the topology
        KStream<String, Transaction> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), transactionAvroSerde));

        KStream<String, EnrichedTransaction> enrichedStream = inputStream.transform(Enricher::new);

        enrichedStream.to(outputTopic, Produced.with(Serdes.String(), enrichedTransactionAvroSerde));

        // Build and return the topology
        return builder.build();
    }
}
