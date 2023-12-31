import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class AvroEnrichmentApplication {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-enrichment-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AvroSerdesUtils.getGenericAvroSerde().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Create the state store
        StoreBuilder<KeyValueStore<String, GenericRecord>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("my-state-store"),
                        Serdes.String(),
                        AvroSerdesUtils.getGenericAvroSerde()
                );
        builder.addStateStore(storeBuilder);

        // Read the input stream
        KStream<String, GenericRecord> inputStream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), AvroSerdesUtils.getGenericAvroSerde())
        );

        // Enrich the input stream using the state store
        KStream<String, GenericRecord> enrichedStream = inputStream.transformValues(
                () -> new EnrichmentTransformer("my-state-store"),
                "my-state-store"
        );

        // Write the enriched stream to the output topic
        enrichedStream.to(
                "output-topic",
                Produced.with(Serdes.String(), AvroSerdesUtils.getGenericAvroSerde())
        );

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}
