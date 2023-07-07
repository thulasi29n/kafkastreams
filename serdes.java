import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class AvroSerdes {

    public static SpecificAvroSerde<TransactionAvro> getTransactionAvroSerde() {
        SpecificAvroSerde<TransactionAvro> serde = new SpecificAvroSerde<>();
        // Configure the serde properties, e.g., schema registry URL
        serde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);
        return serde;
    }

    public static SpecificAvroSerde<InstrumentAvro> getInstrumentAvroSerde() {
        SpecificAvroSerde<InstrumentAvro> serde = new SpecificAvroSerde<>();
        // Configure the serde properties, e.g., schema registry URL
        serde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);
        return serde;
    }

    public static SpecificAvroSerde<EnrichedTransactionAvro> getEnrichedTransactionAvroSerde() {
        SpecificAvroSerde<EnrichedTransactionAvro> serde = new SpecificAvroSerde<>();
        // Configure the serde properties, e.g., schema registry URL
        serde.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);
        return serde;
    }
}
