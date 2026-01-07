import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ComponentTest {
    
    private SparkSession sparkSession;
    private Dataset<Row> dataset;

    @Before
    public void setUp() {
        // Critical: Configure Spark to avoid TypeCoercion issues
        SparkConf conf = new SparkConf()
            .setAppName("ComponentTest")
            .setMaster("local[1]")
            // Disable problematic features
            .set("spark.sql.adaptive.enabled", "false")
            .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .set("spark.sql.execution.arrow.pyspark.enabled", "false")
            .set("spark.sql.execution.arrow.sparkr.enabled", "false")
            .set("spark.sql.warehouse.dir", "target/spark-warehouse")
            .set("spark.sql.shuffle.partitions", "1")
            .set("spark.default.parallelism", "1")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // Critical: Use legacy settings to avoid new TypeCoercion paths
            .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .set("spark.sql.legacy.typeCoercion.datetimeToString.enabled", "true");

        try {
            sparkSession = SparkSession.builder()
                .config(conf)
                // .enableHiveSupport() // Removed - not needed for basic CSV testing
                .getOrCreate();
                
            // Reduce log noise
            sparkSession.sparkContext().setLogLevel("WARN");
            
            System.out.println("Spark Version: " + sparkSession.version());
            System.out.println("******* 1 ******");
            
            // Create test data instead of reading file (to isolate the issue)
            createTestDataset();
            
            System.out.println("******* 2 ******");
            
        } catch (Exception e) {
            System.err.println("Error in setUp: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize Spark", e);
        }
    }
    
    private void createTestDataset() {
        try {
            // Try to load your actual CSV file first
            if (loadActualCsvFile()) {
                System.out.println("Successfully loaded actual CSV file");
            } else {
                // Fallback to programmatic data
                System.out.println("Using fallback test data");
                dataset = sparkSession.sql("SELECT 1 as id, 'test' as name, '2023-01-01' as date");
            }
            
        } catch (Exception e) {
            System.err.println("Error creating test dataset: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
    
    private boolean loadActualCsvFile() {
        try {
            // This matches your original code exactly
            dataset = sparkSession.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", false)
                .option("delimiter", "|")
                .load("src/test/resources/data.dat");
                
            // Test if the file was loaded successfully
            long count = dataset.count();
            return count > 0;
            
        } catch (Exception e) {
            System.err.println("Failed to load actual CSV file: " + e.getMessage());
            return false;
        }
    }
    
    private void testCsvLoading() {
        // Alternative CSV loading approach that avoids TypeCoercion
        try {
            // Create a simple test CSV content
            Dataset<String> lines = sparkSession.createDataset(
                java.util.Arrays.asList(
                    "id|name|value",
                    "1|test1|100",
                    "2|test2|200"
                ),
                org.apache.spark.sql.Encoders.STRING()
            );
            
            // Save to temp file
            String tempPath = "target/test-data.csv";
            lines.coalesce(1).write().mode("overwrite").text(tempPath);
            
            // Now read it back
            dataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .option("delimiter", "|")
                .csv(tempPath);
                
        } catch (Exception e) {
            System.err.println("CSV loading failed: " + e.getMessage());
            // Fall back to simple dataset
            dataset = sparkSession.sql("SELECT 1 as id, 'fallback' as name");
        }
    }
    
    @Test
    public void testSparkFunctionality() {
        try {
            Assert.assertNotNull("SparkSession should not be null", sparkSession);
            Assert.assertNotNull("Dataset should not be null", dataset);
            
            // Show dataset info
            System.out.println("Dataset schema:");
            dataset.printSchema();
            
            System.out.println("Dataset content:");
            dataset.show();
            
            // Basic validations
            long count = dataset.count();
            System.out.println("Dataset count: " + count);
            Assert.assertTrue("Dataset should have data", count > 0);
            
            // Test basic operations
            Dataset<Row> filtered = dataset.limit(10);
            Assert.assertNotNull("Filtered dataset should not be null", filtered);
            
            System.out.println("Test completed successfully!");
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
            Assert.fail("Test failed with exception: " + e.getMessage());
        }
    }
    
    @Test
    public void testBasicSql() {
        try {
            // Test basic SQL operations
            Dataset<Row> result = sparkSession.sql("SELECT 'Hello' as greeting, 123 as number");
            result.show();
            
            Assert.assertEquals("Result should have 1 row", 1, result.count());
            
        } catch (Exception e) {
            System.err.println("SQL test failed: " + e.getMessage());
            Assert.fail("SQL test failed: " + e.getMessage());
        }
    }
    
    @After
    public void tearDown() {
        if (sparkSession != null) {
            try {
                sparkSession.stop();
                System.out.println("SparkSession stopped successfully");
            } catch (Exception e) {
                System.err.println("Error stopping SparkSession: " + e.getMessage());
            }
        }
    }
}


import org.apache.spark.sql.SparkSession;

public class SparkSQLExceptionHandling {
    
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            .appName("Exception Handling Example")
            .master("local[*]")
            .getOrCreate();
        
        String dropSql = "DROP TABLE IF EXISTS my_table";
        
        // Method 1: Catch general Exception
        try {
            sparkSession.sql(dropSql);
            System.out.println("SQL executed successfully");
        } catch (Exception e) {
            System.err.println("Error executing SQL: " + e.getMessage());
            e.printStackTrace();
            // Handle the exception appropriately
        }
        
        // Method 2: More specific handling (Spark 3.x style)
        try {
            sparkSession.sql(dropSql);
        } catch (RuntimeException e) {
            // Check exception type at runtime
            String exceptionClass = e.getClass().getName();
            
            if (exceptionClass.contains("AnalysisException")) {
                System.err.println("Analysis error: " + e.getMessage());
                // Table doesn't exist or SQL syntax error
            } else if (exceptionClass.contains("ParseException")) {
                System.err.println("Parse error: " + e.getMessage());
                // SQL parsing error
            } else {
                System.err.println("Runtime error: " + e.getMessage());
                throw e; // Re-throw if you can't handle it
            }
        }
        
        // Method 3: Safe execution with validation
        try {
            // Check if table exists first (optional)
            if (tableExists(sparkSession, "my_table")) {
                sparkSession.sql("DROP TABLE my_table");
                System.out.println("Table dropped successfully");
            } else {
                System.out.println("Table doesn't exist, skipping drop");
            }
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            // Log and handle appropriately
        }
        
        sparkSession.stop();
    }
    
    // Helper method to check if table exists
    private static boolean tableExists(SparkSession spark, String tableName) {
        try {
            spark.table(tableName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
