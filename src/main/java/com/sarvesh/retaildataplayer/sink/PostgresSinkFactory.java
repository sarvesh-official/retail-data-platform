package com.sarvesh.retaildataplayer.sink;

import com.sarvesh.retaildataplayer.model.SalesOrder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;

public class PostgresSinkFactory {

    public static void addSink(DataStream<SalesOrder> stream,
                               String dbUrl, String dbUser, String dbPassword) {

        String upsertSql =
                "INSERT INTO sales_orders (order_id, category, sub_category, " +
                "quantity, unit_price, total_amount, order_tier, processed_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, NOW()) " +
                "ON CONFLICT (order_id, sub_category) " +
                "DO UPDATE SET " +
                "  category = EXCLUDED.category, " +
                "  quantity = EXCLUDED.quantity, " +
                "  unit_price = EXCLUDED.unit_price, " +
                "  total_amount = EXCLUDED.total_amount, " +
                "  order_tier = EXCLUDED.order_tier, " +
                "  processed_at = NOW();";

        JdbcStatementBuilder<SalesOrder> statementBuilder = (PreparedStatement ps, SalesOrder order) -> {
            ps.setString(1, order.orderId());
            ps.setString(2, order.category());
            ps.setString(3, order.subCategory());
            ps.setInt(4, order.quantity());
            ps.setDouble(5, order.unitPrice());
            ps.setDouble(6, order.totalAmount());
            ps.setString(7, order.orderTier());
        };

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(200)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions
                .JdbcConnectionOptionsBuilder()
                .withUrl(dbUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(dbUser)
                .withPassword(dbPassword)
                .build();

        SinkFunction<SalesOrder> sink = JdbcSink.sink(
                upsertSql,
                statementBuilder,
                executionOptions,
                connectionOptions
        );

        stream.addSink(sink).name("postgres-sink");
    }
}
