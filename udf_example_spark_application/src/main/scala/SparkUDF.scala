import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SparkUDF{
def main(args : Array[String])={

val spark = SparkSession.builder()
                        .appName("SparkUDF")
                        .enableHiveSupport()
                        .getOrCreate()

val df = spark.read
              .format("csv")
              .option("inferSchema","true")
              .option("header","true")
              .option("nullValue","NA")
              .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
              .option("mode","PERMISSSIVE")
              .load(args(0)) 
df.createTempView("survey")

val parseGender = (s : String) => {
                  if(List("cis female", "f", "female", "woman", "femake", "femail", "cis-female", "f").contains(s.toLowerCase))
                    "Female"
else if(List("male", "m", "man", "male-ish", "maile", "malr", "mal", "male (cis)", "make", "male ", "msle", "mail", "cis man", "cis male").contains(s.toLowerCase))
            "Male"
else
    "Transgender"
}

spark.udf.register("pgender", parseGender)

spark.sql(""" select distinct pgender(gender) as parsed_gender from survey""")
      .write
      .saveAsTable("genders")
  
spark.stop()    

}
} 
