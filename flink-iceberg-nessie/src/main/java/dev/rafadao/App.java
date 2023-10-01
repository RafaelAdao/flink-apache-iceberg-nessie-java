package dev.rafadao;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class App {
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Missing s3 endpoint argument");
        }
        String s3Endpoint = args[0];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tableEnv.executeSql(
                "CREATE CATALOG iceberg WITH ("
                + "'type'='iceberg',"
                + "'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
                + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
                + "'uri'='http://catalog:19120/api/v1',"
                + "'authentication-type'='none',"
                + "'ref'='main',"
                + "'client.assume-role.region'='us-east-1',"
                + "'warehouse'='s3://warehouse',"
                + "'s3.endpoint'='"+s3Endpoint+"'"
                + ")");

        // List all catalogs
        TableResult result = tableEnv.executeSql("SHOW CATALOGS");

        result.print();

        // set the current catalog to the new catalog
        tableEnv.useCatalog("iceberg");

        // create a database in the current catalog
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS db");

        // create the table
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS db.table1 (id BIGINT COMMENT 'unique id', data STRING)");

        // create a DataStram of Tuple2 (equivalent to row of 2 fields)
        DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
                Tuple2.of(1L, "foo"),
                Tuple2.of(1L, "bar"),
                Tuple2.of(1L, "baz"));

        // apply a map transformation to convert the Tuple2 to an ExampleData object
        DataStream<ExampleData> mappedDataStream = dataStream.map(
                new MapFunction<Tuple2<Long, String>, ExampleData>() {
                    @Override
                    public ExampleData map(Tuple2<Long, String> value) throws Exception {
                        return new ExampleData(value.f0, value.f1);
                    }
                }
        );

        // convert the DataStream to a Table
        Table table = tableEnv.fromDataStream(mappedDataStream, $("id"), $("data"));


        // register the Table as a temporary view
        tableEnv.createTemporaryView("my_datastream", table);


        // write the DataStream to the table
        tableEnv.executeSql(
                "INSERT INTO db.table1 SELECT * FROM my_datastream");
    }
}
