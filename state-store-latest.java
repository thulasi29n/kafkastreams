import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class StateStoreExample {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-example");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<KeyValueStore<String, Instrument>> instrumentStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("instrument-store"),
                        Serdes.String(),
                        new JsonSerde<>(Instrument.class)
                );

        builder.addStateStore(instrumentStoreBuilder);

        KTable<String, Instrument> instrumentTable = builder.table(
                "instrument-topic",
                Consumed.with(Serdes.String(), new JsonSerde<>(Instrument.class)),
                Materialized.<String, Instrument>as("instrument-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(Instrument.class))
        );

        KStream<String, Transaction> transactionStream = builder.stream("transaction-topic");

        KStream<String, EnrichedTransaction> enrichedTransactionStream = transactionStream.transformValues(
                () -> new InstrumentLookupTransformer("instrument-store"),
                "instrument-store"
        );

        enrichedTransactionStream
                .map((key, value) -> KeyValue.pair(value.getEnrichedKey(), value.getEnrichedValue()))
                .to("enriched-transaction-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
