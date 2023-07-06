import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import com.example.avro.TransactionAvro;
import com.example.avro.InstrumentAvro;
import com.example.avro.EnrichedInstrumentAvro;

import java.time.Duration;
import java.util.Properties;

public class EnrichmentJoin {
    public static void main(String[] args) {
        // Set up the configuration properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrichment-join-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create the StreamsBuilder instance
        StreamsBuilder builder = new StreamsBuilder();

        // Create the KStream for the input topic
        KStream<String, TransactionAvro> transactionStream = builder.stream("input-topic",
                Consumed.with(Serdes.String(), AvroSerdeUtils.getGenericAvroSerde(TransactionAvro.class)))
                .selectKey((key, value) -> value.getIsin()); // Repartition by the new key

        // Create the KTable for the state store
        KTable<String, InstrumentAvro> stateStoreTable = builder.table("state-store-topic",
                Consumed.with(Serdes.String(), AvroSerdeUtils.getGenericAvroSerde(InstrumentAvro.class)));

        // Join the transaction stream with the state store table within a time window
        KStream<String, EnrichedInstrumentAvro> enrichedStream = transactionStream
                .join(stateStoreTable,
                        (transactionAvro, instrumentAvro) -> {
                            EnrichedInstrumentAvro enrichedInstrumentAvro = new EnrichedInstrumentAvro();
                            enrichedInstrumentAvro.setId(transactionAvro.getId());
                            enrichedInstrumentAvro.setName(transactionAvro.getName());
                            enrichedInstrumentAvro.setValue(transactionAvro.getValue());
                            enrichedInstrumentAvro.setInstrumentId(instrumentAvro.getInstrumentId());
                            enrichedInstrumentAvro.setInstrumentName(instrumentAvro.getInstrumentName());
                            enrichedInstrumentAvro.setInstrumentValue(instrumentAvro.getInstrumentValue());
                            // Set other attributes from transactionAvro and instrumentAvro
                            return enrichedInstrumentAvro;
                        },
                        JoinWindows.of(Duration.ofMinutes(5)) // Set the time window duration
                );

        // Write the enriched stream to the output topic
        enrichedStream.to("output-topic",
                Produced.with(Serdes.String(), AvroSerdeUtils.getGenericAvroSerde(EnrichedInstrumentAvro.class)));

        // Get the topology description and print it
        Topology topology = builder.build();
        System.out.println(topology.describe());

        // Create and register a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Close the KafkaStreams instance on shutdown
            streams.close();
        }));

        // Set up exception handling
        KafkaStreamsExceptionHandler exceptionHandler = (exception, data) -> {
            // Handle the exception
            System.err.println("Exception caught during processing: " + exception.getMessage());
            // You can choose to log, retry, or skip the record here
            // For example,Apologies for the incomplete response. Here's the rest of the code:

```java
            // Log the error
            exception.printStackTrace();
            // Skip the record by returning null
            return null;
        };

        // Build the KafkaStreams instance with the exception handler
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.setUncaughtExceptionHandler(exceptionHandler);

        // Start the KafkaStreams application
        streams.start();
    }
}
