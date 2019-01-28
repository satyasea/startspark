package startspark.apache.getstart;
// Running SQL Queries Programmatically
import org.apache.commons.collections.set.SynchronizedSortedSet;
import org.apache.spark.sql.*;
import org.apache.spark.sql.AnalysisException;

public class JavaSparkSQLExample {

    private static String dir = "C:\\Users\\wolf\\IdeaProjects\\startspark\\";
    private static String inputFile = "people.json";
    private static String inputURL = dir+inputFile;

    public static void main (String []args ){

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQL")
                .config("spark.master", "local")
                .getOrCreate();

        DataFrameReader dfr = new DataFrameReader(spark);

        Dataset<Row> df = dfr.json(inputURL);
        System.out.println("df.show()");
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people where name='Andy'");
        sqlDF.show();
        spark.close();

       // Global Temporary View

        SparkSession  spark2 = SparkSession
                .builder()
                .appName("SparkSQL")
                .config("spark.master", "local")
                .getOrCreate();
        DataFrameReader dfr2 = new DataFrameReader(spark2);

        Dataset<Row> df2 = dfr2.json(inputURL);

        // Register the DataFrame as a global temporary view catch exception
        try {
            df2.createGlobalTempView("people");
        }
        catch (AnalysisException aex )  {
            System.out.println(aex.getMessage());
            }

        // Global temporary view is tied to a system preserved database `global_temp`
        spark2.sql("SELECT name FROM global_temp.people").show();

    // Global temporary view is cross-session
        spark2.newSession().sql("SELECT * FROM global_temp.people").show();

        spark2.close();
    }

}
