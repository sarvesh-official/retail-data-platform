package com.sarvesh.retaildataplayer.functions;

import com.sarvesh.retaildataplayer.model.SalesOrder;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CsvLineParser implements MapFunction<String, SalesOrder> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvLineParser.class);

    @Override
    public SalesOrder map(String line) throws Exception {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }

        String[] fields = line.split(",", -1);

        if (fields.length != 5) {
            LOG.warn("Malformed line (expected 5 fields, got {}): {}", fields.length, line);
            return null;
        }

        try {
            String orderId = fields[0].trim();
            String category = fields[1].trim();
            String subCategory = fields[2].trim();
            int quantity = Integer.parseInt(fields[3].trim());
            double unitPrice = Double.parseDouble(fields[4].trim());

            // totalAmount and orderTier are derived — set to defaults here, the enricher fills them in
            return new SalesOrder(orderId, category, subCategory, quantity, unitPrice, 0.0, "");
        } catch (NumberFormatException e) {
            LOG.warn("Non-numeric value in line: {}", line);
            return null;
        }
    }
}
