package com.github.sioncheng.xpns.dispatcher;

import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

public class HBaseClientTest {

    @Test
    public void testPutGetScan() throws IOException  {
        String to = "1_aaaaa000001";
        Notification notification = new Notification();
        notification.setTo(to);
        notification.setTitle("title");
        notification.setBody("body");
        notification.setExt(new JSONObject());
        notification.setUniqId("testnoti1232343240");

        NotificationEntity entity = new NotificationEntity();
        entity.setCreateDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));
        entity.setTtl(3600);
        entity.setStatus(NotificationEntity.NEW);
        entity.setStatusDateTime(entity.getCreateDateTime());

        try (Connection connection = ConnectionFactory.createConnection()) {
            Assert.assertNotNull(connection);

            String rowKey = StringUtils.reverse(notification.getTo()) +
                    notification.getUniqId();
            Put put = new Put(rowKey.getBytes());

            byte[] family = "cf".getBytes();

            put.addColumn(family, "to".getBytes(), notification.getTo().getBytes());
            put.addColumn(family, "ttl".getBytes(), String.valueOf(entity.getTtl()).getBytes());
            put.addColumn(family, "createDateTime".getBytes(), entity.getCreateDateTime().getBytes());
            put.addColumn(family, "status".getBytes(), String.valueOf(entity.getStatus()).getBytes());
            put.addColumn(family, "statusDateTime".getBytes(), entity.getStatusDateTime().getBytes());
            put.addColumn(family, "notification".getBytes(), notification.toJSONObject().toJSONString().getBytes("UTF-8"));

            Table table = connection.getTable(TableName.valueOf("notification"));
            table.put(put);



            Get get = new Get(rowKey.getBytes());
            Result result = table.get(get);

            Assert.assertTrue(result.advance());

            printColumn(family,"to", result);
            printColumn(family, "ttl", result);
            printColumn(family, "createDateTime", result);
            printColumn(family, "status", result);
            printColumn(family, "statusDateTime", result);
            printColumn(family, "notification", result);

            Scan scan = new Scan();
            scan.setRowPrefixFilter(StringUtils.reverse(to).getBytes());
            SingleColumnValueFilter statusFilter = new SingleColumnValueFilter("cf".getBytes(),
                    "status".getBytes(),
                    CompareFilter.CompareOp.EQUAL,
                    String.valueOf(NotificationEntity.NEW).getBytes());
            scan.setFilter(statusFilter);

            ResultScanner scanner = table.getScanner(scan);
            Result result1 = scanner.next();
            Assert.assertNotNull(result1);

            printColumn(family,"to", result1);
            printColumn(family, "ttl", result1);
            printColumn(family, "createDateTime", result1);
            printColumn(family, "status", result1);
            printColumn(family, "statusDateTime", result1);
            printColumn(family, "notification", result1);


            table.close();
        }
    }

    private void printColumn(byte[] family, String qualifier, Result result) {

        System.out.println(qualifier
                + " = "
                + new String(result.getValue(family, qualifier.getBytes())));
    }
}
