package com.kafka.stream.greetings.topology;

import com.kafka.stream.greetings.domain.Order;
import com.kafka.stream.greetings.domain.OrderType;
import com.kafka.stream.greetings.domain.Revenue;
import com.kafka.stream.greetings.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class OrdersTopology {

    private OrdersTopology(){}

    public static final  String ORDERS = "orders";
    public static final String STORE = "stores";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";

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

                            generalOrderStream
                                    .mapValues((readonlyKey, value) -> revenueValueMapper.apply(value))
                                    .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory
                                            .revenueSerdesUsingGeneric()));
                        })
                ).branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream
                                    .print(Printed.<String, Order>toSysOut()
                                    .withLabel("restaurantOrderStream"));

                            restaurantOrderStream
                                    .mapValues((readonlyKey, value) -> revenueValueMapper.apply(value))
                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory
                                            .revenueSerdesUsingGeneric()));
                        })
                        );

        return streamsBuilder.build();
    }
}
