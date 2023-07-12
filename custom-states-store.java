public class StateBuilder {
    public static StoreBuilder<KeyValueStore<String, Value>> createStateBuilder(String stateStoreName, Serde<Value> valueSerde) {
        return Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(stateStoreName),
            Serdes.String(),
            valueSerde
        );
    }

    public static void populateStateStore(String topicName, StreamsBuilder builder, Serde<Instrument> instrumentSerde) {
        KStream<String, Instrument> instrumentStream = builder.stream(topicName);
        instrumentStream.foreach((key, instrument) -> {
            KeyValueStore<String, Instrument> stateStore = builder
                .getStateStore("my-state-store");
            stateStore.put(key, instrument);
        });
    }
}
