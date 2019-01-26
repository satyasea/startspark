package startspark.apache.getstart;
// http://spark.apache.org/docs/2.4.0/sql-getting-started.html#creating-dataframes
//Untyped Dataset Operations
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameOperations {

    private static String dir = "C:\\Users\\wolf\\IdeaProjects\\startspark\\";
    private static String inputFile = "SOURCE3.json";
    private static String inputURL = dir+inputFile;

    public static void main (String []args ){

        SparkSession spark = SparkSession
                .builder()
                .appName("Create Data Frame")
                //required to run on local machine
                .config("spark.master", "local")
                .getOrCreate();

        DataFrameReader dfr = new DataFrameReader(spark);


         Dataset<Row> df = dfr.json(inputURL);
         System.out.println("df.show()");
       df.show();
        // Print the schema in a tree format
        System.out.println("df.printSchema()");
        df.printSchema();
        //other dataframe operations
       System.out.println("df.select(\"base\").show()");
        df.select("date").show();
       System.out.println("df.groupBy(\"date\").count().show()");
        df.groupBy("date").count().show();

        spark.close();
    }

}
