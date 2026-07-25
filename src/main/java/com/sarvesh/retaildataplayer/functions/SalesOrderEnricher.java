package com.sarvesh.retaildataplayer.functions;

import com.sarvesh.retaildataplayer.model.SalesOrder;
import org.apache.flink.api.common.functions.MapFunction;


public class SalesOrderEnricher implements MapFunction<SalesOrder, SalesOrder> {

    private static final double LOW_TIER_THRESHOLD = 100.0;
    private static final double MEDIUM_TIER_THRESHOLD = 1000.0;

    @Override
    public SalesOrder map(SalesOrder order) throws Exception {
        double totalAmount = order.quantity() * order.unitPrice();
        String tier = classifyTier(totalAmount);

        return new SalesOrder(
                order.orderId(),
                order.category(),
                order.subCategory(),
                order.quantity(),
                order.unitPrice(),
                totalAmount,
                tier
        );
    }

    private String classifyTier(double totalAmount) {
        if (totalAmount < LOW_TIER_THRESHOLD) {
            return "LOW";
        } else if (totalAmount < MEDIUM_TIER_THRESHOLD) {
            return "MEDIUM";
        } else {
            return "HIGH";
        }
    }
}
