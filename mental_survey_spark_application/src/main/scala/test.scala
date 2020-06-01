package harsh
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SparkTestApp{
 def main(args: Array[String])={
  val spark = SparkSession.builder()
               .appName("SparkTestApp")
               .enableHiveSupport()
               .getOrCreate()

val df = spark.read.
                    format("csv")
                    .option("header","true")
                    .option("inferSchema","true")
                    .option("timestampFormat","yyyy-MM-dd'T'HH:mm;ss")
                    .option("nullValue", "NA")
                    .option("mode", "PERMISSIVE")
                    .load(args(0)) 

df.createTempView("survey")
spark.sql(""" select Age, count(*) as frequency 
              from survey where Age  between 20 and 65
               group by Age
              """)
      .write
      .saveAsTable("surveyFrequency")
 
spark.stop()
}
}         
