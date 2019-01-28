package startspark.apache.getstart;
// http://spark.apache.org/docs/2.4.0/sql-getting-started.html#creating-dataframes

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CreateDataFrame {

    private static String dir = "C:\\Users\\wolf\\IdeaProjects\\startspark\\";
    private static String inputFile = "people.json";
    private static String inputURL = dir+inputFile;

    public static void main (String []args ){

        SparkSession spark = SparkSession
                .builder()
                .appName("Create Data Frame")
                //required to run on local machine
                .config("spark.master", "local")
                .getOrCreate();

       // Dataset<Row> df = spark.read().json(inputURL );
        DataFrameReader dfr = new DataFrameReader(spark);
         Dataset<Row> df = dfr.json(inputURL);
        // Displays the content of the DataFrame to stdout
        df.show();

    }



}
