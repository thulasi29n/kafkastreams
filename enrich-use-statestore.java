import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class AvroEnrichmentApplication {

    public static void main(String[] args) {
        // Initialize Kafka Streams configuration
        Properties config = new Properties();
        // Set the necessary configuration properties

        StreamsBuilder builder = new StreamsBuilder();

        // Create the input stream
        KStream<String, TransactionAvro> inputStream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), avroSerde)
        );

        // Repartition the input stream based on the new key
        KStream<String, TransactionAvro> repartitionedStream = inputStream.selectKey(
                (key, value) -> value.getIsin() + value.getTradeDate()
        );

        // Enrich the repartitioned stream using the state store
        KStream<String, EnrichedTransactionAvro> enrichedStream = repartitionedStream.transformValues(
                new EnrichmentTransformer("my-state-store"),
                "my-state-store"
        );

        // Write the enriched stream to the output topic
        enrichedStream.to(
                "output-topic",
                Produced.with(Serdes.String(), avroSerde)
        );

        // Build and start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    // Rest of the code remains the same as before
}


 import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;

public class EnrichmentTransformer implements ValueTransformerWithKeySupplier<String, TransactionAvro, EnrichedTransactionAvro> {

    private final String stateStoreName;
    private KeyValueStore<String, InstrumentAvro> stateStore;
    private ProcessorContext context;

    public EnrichmentTransformer(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.stateStore = (KeyValueStore<String, InstrumentAvro>) context.getStateStore(stateStoreName);
    }

    @Override
    public EnrichedTransactionAvro transform(String key, TransactionAvro transaction) {
        // Perform the enrichment logic using the state store
        InstrumentAvro instrument = stateStore.get(key);

        if (instrument != null) {
            // Create a new EnrichedTransactionAvro by combining the TransactionAvro and InstrumentAvro
            EnrichedTransactionAvro enrichedTransaction = new EnrichedTransactionAvro();
            enrichedTransaction.setId(transaction.getId());
            enrichedTransaction.setAmount(transaction.getAmount());
            enrichedTransaction.setInstrumentId(instrument.getId());
            enrichedTransaction.setInstrumentName(instrument.getName());
            enrichedTransaction.setInstrumentValue(instrument.getValue());
            enrichedTransaction.setInstrumentCountry(instrument.getCountry());

            return enrichedTransaction;
        }

        // If no matching instrument is found, return null or a default value depending on your requirement
        return null;
    }

    @Override
    public void close() {
        // Perform any necessary cleanup
    }

    @Override
    public ValueTransformerWithKey<String, TransactionAvro, EnrichedTransactionAvro> get() {
        return this;
    }
}
