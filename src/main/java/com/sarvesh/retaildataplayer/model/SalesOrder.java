package com.sarvesh.retaildataplayer.model;

public record SalesOrder(
        String orderId,
        String category,
        String subCategory,
        int quantity,
        double unitPrice,
        double totalAmount,
        String orderTier
) {}
