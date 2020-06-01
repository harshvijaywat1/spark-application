package sparkUDFlib
import org.apache.spark.sql.api.java.UDF1

class ParseGender extends UDF1[String, String]{

def call(s: String): String = {
 if(List("cis female", "f", "female", "woman", "femake", "femail", "cis-female", "f").contains(s.toLowerCase))
                    "Female"
else if(List("male", "m", "man", "male-ish", "maile", "malr", "mal", "male (cis)", "make", "male ", "msle", "mail", "cis man", "cis male").contains(s.toLowerCase))
            "Male"
else
    "Transgender"
}
}
  
