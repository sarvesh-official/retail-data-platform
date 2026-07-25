package com.sarvesh.retaildataplayer.functions;

import com.sarvesh.retaildataplayer.model.SalesOrder;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesOrderValidator implements FilterFunction<SalesOrder> {

    private static final Logger LOG = LoggerFactory.getLogger(SalesOrderValidator.class);

    @Override
    public boolean filter(SalesOrder order) throws Exception {
        if (order == null) {
            return false;
        }

        if (order.orderId() == null || order.orderId().isEmpty()) {
            LOG.warn("Dropping record: empty orderId");
            return false;
        }

        if (order.category() == null || order.category().isEmpty()) {
            LOG.warn("Dropping record {}: empty category", order.orderId());
            return false;
        }

        if (order.subCategory() == null || order.subCategory().isEmpty()) {
            LOG.warn("Dropping record {}: empty subCategory", order.orderId());
            return false;
        }

        if (order.quantity() <= 0) {
            LOG.warn("Dropping record {}: quantity {} must be > 0", order.orderId(), order.quantity());
            return false;
        }

        if (order.unitPrice() < 0) {
            LOG.warn("Dropping record {}: unitPrice {} must be >= 0", order.orderId(), order.unitPrice());
            return false;
        }

        return true;
    }
}
