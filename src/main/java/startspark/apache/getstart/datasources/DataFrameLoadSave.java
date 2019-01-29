package startspark.apache.getstart.datasources;
// http://spark.apache.org/docs/2.4.0/sql-getting-started.html#creating-dataframes

import org.apache.spark.sql.*;

public class DataFrameLoadSave {

    // resources are not in directory "examples/src/main/resources/   but rather one dir down at src
    // will error if file exists that its trying to save.
   // add into file save function:  .write().mode(SaveMode.Overwrite)

    private static String resources = "src/main/resources/";

    public static void main (String []args ){

        SparkSession spark = SparkSession
                .builder()
                .appName("DF load and save")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> usersDF = spark.read().load(resources + "users.parquet");

        System.out.println("saving from json load to new names and colors parquet file");

        usersDF.select("name", "favorite_color").write().mode(SaveMode.Overwrite).save(resources + "namesAndFavColors.parquet");
        System.out.println("  usersDF");
        usersDF.show();

        Dataset<Row> peopleDF=
                spark.read().format("json").load(resources + "people.json");
        System.out.println("  peopleDF");
        peopleDF.show();

        // will error if file exists that its trying to save.
        System.out.println("saving from json load to new names and ages parquet file");
        peopleDF.select("name", "age").write().mode(SaveMode.Overwrite).format("parquet").save(resources + "namesAndAges.parquet");


        Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(resources + "people.csv");

        System.out.println("peopleDFCsv");
        peopleDF.show();



        usersDF.write().mode(SaveMode.Overwrite).format("orc")
                .option("orc.bloom.filter.columns", "favorite_color")
                .option("orc.dictionary.key.threshold", "1.0")
                .save(resources +"users_with_options.orc");



        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`" + resources + "users.parquet`");

        System.out.println("sqlDF");
        sqlDF.show();


        //saving to persistent storage
        //can only run once with same tablename, or else it will fail if table file exists
        peopleDF.write().mode(SaveMode.Overwrite).bucketBy(42, "name").sortBy("age").saveAsTable("x_people_bucketed");


        usersDF.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("favorite_color")
                .format("parquet")
                .save(resources +"namesPartByColor.parquet");

        // here is a mistake, wouldn't compile because this should be users not people
      /*
      peopleDF
  .write()
  .partitionBy("favorite_color")
  .bucketBy(42, "name")
  .saveAsTable("people_partitioned_bucketed");
       */
        usersDF.write()
                .partitionBy("favorite_color")
                .bucketBy(42, "name")
                .saveAsTable("x_users_partitioned_bucketed");

        spark.close();
    }
}
