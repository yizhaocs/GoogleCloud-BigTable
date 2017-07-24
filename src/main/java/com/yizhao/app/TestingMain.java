package com.yizhao.app;


import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;


/**
 * build it:
 * mvn clean package
 * scp /Users/yzhao/IdeaProjects/ENG835_Backfill/target/Backfill-jar-with-dependencies.jar
 * run it:
 * /usr/java/jdk/bin/java -jar Backfill-jar-with-dependencies.jar
 *
 */

/**
 * aerospike: // total time used:2824 milliseconds ,with count:2154, 1.31104921 ms per request
 * big table: // total time used:114714 milliseconds ,with count:2154, 53.2562674 ms per request
 */
public class TestingMain {

    static Map<String, Map<Integer,KeyValueTs>> map = new HashMap<String, Map<Integer, KeyValueTs>>();
    static int totalIteration = 1000;
    static byte[] columnFaimilyName = "columnFaimilyName".getBytes();
    static String bigtableTableName = "table15";
    static byte[] rowKey = "rowKey".getBytes();

    public static void main(String[] args) throws Exception{
        ProcessCkvData.readThenWrite(map, "src/main/resources/20170712-004428.ps101-lax1.0000000000010309020.csv");
        System.out.println(map.size());

        //writeToBigTableInt();
        //readBigTableInt();

        writeToBigTableCKVmap( );
        readBigTableMap();
    }



    public static void writeToBigTableCKVmap( ){
        try {
            byte[] tableName = Bytes.toBytes(bigtableTableName);

            Connection connection = BigTableConnection.getConnection();
            //System.out.println(BigtableHelloWorld.create(connection));
            System.out.println(CreateTable.execute(connection, tableName, columnFaimilyName));
            Table table = connection.getTable(TableName.valueOf(tableName));

            long startTime = System.nanoTime();

            int count = 0;
            for(String cookieId : map.keySet()){
                byte[] columnQualifier = cookieId.getBytes();
                Map<Integer, KeyValueTs > ckvMap = map.get(cookieId);
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(byteOut);
                out.writeObject(ckvMap);
                InsertTable.execute(table, rowKey, columnFaimilyName, columnQualifier, byteOut.toByteArray());
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000; // in milliseconds
            System.out.println("total time used for writing:" + duration + " milliseconds ,with count:" + count);

        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void writeToBigTableInt( ){
        try {
            byte[] tableName = Bytes.toBytes(bigtableTableName);

            Connection connection = BigTableConnection.getConnection();
            //System.out.println(BigtableHelloWorld.create(connection));
            System.out.println(CreateTable.execute(connection, tableName, columnFaimilyName));
            Table table = connection.getTable(TableName.valueOf(tableName));

            long startTime = System.nanoTime();

            int count = 0;
            for(int i = 0; i < totalIteration; i++){
                byte[] columnQualifier = Bytes.toBytes(i);
                InsertTable.execute(table, rowKey, columnFaimilyName, columnQualifier, String.valueOf(i).getBytes());
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000; // in milliseconds
            System.out.println("total time used for writing:" + duration + " milliseconds ,with count:" + count);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void readBigTableInt() throws Exception{
        Connection connection = BigTableConnection.getConnection();
        byte[] tableName = Bytes.toBytes(bigtableTableName);
        byte[] rowKey = "rowKey".getBytes();
        Table table = connection.getTable(TableName.valueOf(tableName));
        int count = 0;
        long startTime = System.nanoTime();
        for(int i = 0; i < totalIteration; i++) {
            byte[] columnQualifier = Bytes.toBytes(i);
             ReadTable.executeReadingArowByItsKey(table, rowKey, columnFaimilyName, columnQualifier);
            // Parse byte array to Map
           // ByteArrayInputStream byteIn = new ByteArrayInputStream(result);
            //ObjectInputStream in = new ObjectInputStream(byteIn);
            //Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
            //System.out.println("cookie:" + cookieId + " ,ckvMap:" + data2.toString());
            count ++;
            //System.out.println(count);
        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("total time used for reading:" + duration + " milliseconds ,with count:" + count);
    }

    public static void readBigTableMap() throws Exception{
        Connection connection = BigTableConnection.getConnection();
        byte[] tableName = Bytes.toBytes(bigtableTableName);
        byte[] rowKey = "rowKey".getBytes();
        Table table = connection.getTable(TableName.valueOf(tableName));
        int count = 0;
        long startTime = System.nanoTime();
        for(String cookieId : map.keySet()) {
            byte[] columnQualifier = cookieId.getBytes();
            ReadTable.executeReadingArowByItsKey(table, rowKey, columnFaimilyName, columnQualifier);
            // Parse byte array to Map
            // ByteArrayInputStream byteIn = new ByteArrayInputStream(result);
            //ObjectInputStream in = new ObjectInputStream(byteIn);
            //Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
            //System.out.println("cookie:" + cookieId + " ,ckvMap:" + data2.toString());
            count ++;
            //System.out.println(count);
        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("total time used for reading:" + duration + " milliseconds ,with count:" + count);
    }
}
