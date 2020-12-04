package com.ibm.gbs.threerivers.kafka;

import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.ibm.gbs.schema.Transaction;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Service
public class ThreeRiversDemoGlobalKTable {

    final String CUSTOMER_TOPIC = "Customer";
    final String REKEYED_CUSTOMER_TOPIC = "RekeyedCustomer";
    final String BALANCE_TOPIC = "Balance";
    final String CUSTOMER_BALANCE_TOPIC = "CustomerBalance";
    final String CONSUMER_GROUP = "three-rivers-demo-globalktable";

    final String CUSTOMER_STORE = "customer-store";

    final String bootstrapServers = "localhost:9092";
    final String schemaRegistryUrl = "http://localhost:8081";

    private KafkaStreams streams;

    public Properties buildStreamsProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, CONSUMER_GROUP);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-global-tables");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);

        return props;
    }

    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Customer> customerStream = builder.<String, Customer>stream(CUSTOMER_TOPIC)
                .map((key, customer) -> new KeyValue<>(customer.getAccountId(), customer));

        customerStream.to(REKEYED_CUSTOMER_TOPIC);

        GlobalKTable<String, Customer> customers = builder.globalTable(REKEYED_CUSTOMER_TOPIC,
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(getCustomerAvroSerde()));

        KStream<String, Transaction> balances = builder.stream(BALANCE_TOPIC);

        KStream<String, CustomerBalance> customerBalance = balances.join(
                customers,
                (accountId,transaction) -> transaction.getAccountId(),
                (balance, customer) -> CustomerBalance.newBuilder()
                .setAccountId(balance.getAccountId())
                .setCustomerId(customer.getCustomerId())
                .setPhoneNumber(customer.getPhoneNumber())
                .setBalance(balance.getBalance())
                .build());

        customerBalance.to(CUSTOMER_BALANCE_TOPIC, Produced.with(Serdes.String(), getCustomerBalanceAvroSerde()));

        return builder.build();
    }

    private SpecificAvroSerde<CustomerBalance> getCustomerBalanceAvroSerde() {
        SpecificAvroSerde<CustomerBalance> customerBalanceAvroSerde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        customerBalanceAvroSerde.configure(serdeConfig, false);
        return customerBalanceAvroSerde;
    }

    private SpecificAvroSerde<Customer> getCustomerAvroSerde() {
        SpecificAvroSerde<Customer> customerAvroSerde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        customerAvroSerde.configure(serdeConfig, false);
        return customerAvroSerde;
    }

    public void start() {
        ThreeRiversDemoGlobalKTable demo = new ThreeRiversDemoGlobalKTable();
        Properties streamProps = demo.buildStreamsProperties();
        Topology topology = demo.buildTopology();

        streams = new KafkaStreams(topology, streamProps);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }

    public void stop() {
        streams.close();
    }
}