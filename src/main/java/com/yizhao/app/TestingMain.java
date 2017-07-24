package com.yizhao.app;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


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

    static int totalIteration = 1000;
    static byte[] columnFaimilyName = "columnFaimilyName".getBytes();
    static String bigtableTableName = "table14";
    static byte[] rowKey = "rowKey".getBytes();

    public static void main(String[] args) throws Exception{
        writeToBigTable( );
        readBigTable();

    }



    public static void writeToBigTable( ){
        try {
            byte[] tableName = Bytes.toBytes(bigtableTableName);

            Connection connection = BigTableConnection.getConnection();
            //System.out.println(BigtableHelloWorld.create(connection));
            System.out.println(CreateTable.execute(connection, tableName, columnFaimilyName));
            Table table = connection.getTable(TableName.valueOf(tableName));
            writeToBigTable2(table);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void writeToBigTable2(Table table ) throws Exception{


        long startTime = System.nanoTime();

        int count = 0;
        for(int i = 0; i < totalIteration; i++){


            byte[] columnQualifier = Bytes.toBytes(i);
            InsertTable.execute(table, rowKey, columnFaimilyName, columnQualifier, String.valueOf(i).getBytes());
/*

            System.out.println("cookie:" + cookieId);
            byte[] result = ReadTable.execute(table, columnFaimilyName, columnQualifier);
            // Parse byte array to Map
            ByteArrayInputStream byteIn = new ByteArrayInputStream(result);
            ObjectInputStream in = new ObjectInputStream(byteIn);
            Map<Integer, String> data2 = (Map<Integer, String>) in.readObject();
            System.out.println("cookie:" + cookieId + " ,ckvMap:" + data2.toString());
*/

            count ++;
           // System.out.println("count:" + count);

        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime)/1000000; // in milliseconds
        System.out.println("total time used for writing:" + duration + " milliseconds ,with count:" + count);
    }

    public static void readBigTable() throws Exception{
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
}
