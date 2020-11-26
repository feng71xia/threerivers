package com.ibm.gbs.threerivers;

import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import com.ibm.gbs.schema.Transaction;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class ThreeRiversDemo {

    final String CUSTOMER_TOPIC = "Customer";
    final String REKEYED_CUSTOMER_TOPIC = "RekeyedCustomer";
    final String BALANCE_TOPIC = "Balance";
    final String REKEYED_BALANCE_TOPIC = "RekeyedBalance";
    final String CUSTOMER_BALANCE_TOPIC = "CustomerBalance";
    final String CONSUMER_GROUP = "three-rivers-demo-ktable";

    final String bootstrapServers = "localhost:9092";
    final String schemaRegistryUrl = "http://localhost:8081";

    public Properties buildStreamsProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, CONSUMER_GROUP);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        return props;
    }

    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Customer> customerStream = builder.<String, Customer>stream(CUSTOMER_TOPIC)
                .map((key, customer) -> new KeyValue<>(customer.getAccountId(), customer));

        customerStream.to(REKEYED_CUSTOMER_TOPIC);

        KTable<String, Customer> customers = builder.table(REKEYED_CUSTOMER_TOPIC);

        KStream<String, Transaction> balancesStream = builder.<String, Transaction>stream(BALANCE_TOPIC)
                .map((key, transaction) -> new KeyValue<>(transaction.getAccountId(), transaction));

        balancesStream.to(REKEYED_BALANCE_TOPIC);

        KStream<String, Transaction> balances = builder.<String, Transaction>stream(REKEYED_BALANCE_TOPIC);

        KStream<String, CustomerBalance> customerBalance = balances.join(customers,(balance, customer) -> CustomerBalance.newBuilder()
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

    public static void main(String[] args) {
        ThreeRiversDemo demo = new ThreeRiversDemo();
        Properties streamProps = demo.buildStreamsProperties();
        Topology topology = demo.buildTopology();

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}