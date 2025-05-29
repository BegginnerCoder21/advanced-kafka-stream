package com.kafka.stream.greetings.topology;

import com.kafka.stream.greetings.domain.Order;
import com.kafka.stream.greetings.domain.OrderType;
import com.kafka.stream.greetings.domain.Revenue;
import com.kafka.stream.greetings.domain.TotalRevenue;
import com.kafka.stream.greetings.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class OrdersTopology {

    private OrdersTopology(){}

    public static final  String ORDERS = "orders";
    public static final String STORE = "stores";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";

    public static Topology buildTopology()
    {
        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);
        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var orderStreams = streamsBuilder.stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerdesUsingGeneric()));

        orderStreams.print(Printed.<String, Order>toSysOut().withLabel("orderStreams"));

        orderStreams.split(Named.as("General-restaurant-stream"))
                .branch(
                        generalPredicate,
                        Branched.withConsumer(generalOrderStream -> {
                            generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("generalOrderStream"));

//                            generalOrderStream
//                                    .mapValues((readonlyKey, value) -> revenueValueMapper.apply(value))
//                                    .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory
//                                            .revenueSerdesUsingGeneric()));

                            aggregateOrderByCount(generalOrderStream, GENERAL_ORDERS_COUNT);
//                            aggregateOrderByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE);
                        })
                ).branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream
                                    .print(Printed.<String, Order>toSysOut()
                                    .withLabel("restaurantOrderStream"));

//                            restaurantOrderStream
//                                    .mapValues((readonlyKey, value) -> revenueValueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory
//                                            .revenueSerdesUsingGeneric()));
                            aggregateOrderByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT);
//                            aggregateOrderByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE);

                        })
                        );

        return streamsBuilder.build();
    }

    private static void aggregateOrderByRevenue(KStream<String, Order> orderStream, String storeName) {

        Initializer<TotalRevenue> initializerTotalRevenue = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        KTable<String, TotalRevenue> aggregateTotalRevenue = orderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdesUsingGeneric()))
                .aggregate(initializerTotalRevenue, aggregator, Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(SerdesFactory.totalRevenueSerdes())
        );

        aggregateTotalRevenue
                .toStream()
                .print(Printed.<String, TotalRevenue>toSysOut()
                .withLabel(storeName));

    }

    private static void aggregateOrderByCount(KStream<String, Order> aggregateOrderStream, String storeName) {

        KTable<String, Long> count = aggregateOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdesUsingGeneric()))
                .count(Named.as(storeName), Materialized.as(storeName));

//        KTable<String, Long> count = aggregateOrderStream
//                .groupBy((key, value)-> value.locationId(),Grouped.with(Serdes.String(), SerdesFactory.orderSerdesUsingGeneric()))
//                .count(Named.as(storeName), Materialized.as(storeName));

        count.toStream().print(Printed.<String, Long>toSysOut().withLabel(storeName));
    }
}
