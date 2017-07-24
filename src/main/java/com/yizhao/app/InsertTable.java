package com.yizhao.app;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * Created by yzhao on 7/17/17.
 */
public class InsertTable {
    public static void execute(Table table, byte[] rowKey, byte[] columnFamilyName, byte[] columnQualifier, byte[] value ) {
        try {
            Put put = new Put(rowKey);
            put.addColumn(columnFamilyName, columnQualifier, value);
            table.put(put);
        } catch (Exception e) {
        }
    }
}
