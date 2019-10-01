/*
 * Program to Analyse the AWS ELB Logs . This processing involves the following sequences 
 * Read File
 * Apply the schema
 * Clean Up and Filter as needed
 * Enrich Data - to enable analytics
 * Perform Analytics
 * 
 * */
import org.apache.spark.sql.SparkSession

object AWSLogAnalyser {

  // This is the schema to impart structure to the unstructured log file. Reference : AWS ELB document 
  case class logSchema(timestamp: String,
    elb_name: String,
    request_ip: String,
    request_port: Int,
    backend_ip: String,
    backend_port: Int,
    request_processing_time: Double,
    backend_processing_time: Double,
    client_response_time: Double,
    elb_response_code: String,
    backend_response_code: String,
    received_bytes: BigInt,
    sent_bytes: BigInt,
    request_verb: String,
    url: String,
    protocol: String,
    user_agent: String,
    ssl_cipher: String,
    ssl_protocol: String);

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("LogAnalyser").getOrCreate()
    /* Reading the log file. As the requirement changes, the file read can be pointed to an hdfs by using the hdfs:/// url
			* Alternatevely, this can be modified to point to a streaming directory as well
			*Note : Using local file system willbe restricted to deploy mode : client 
			* Hardcoding for testing . Can pass the file as command line arguement args(0)
			* 
			*/
    val logFile = spark.sparkContext.textFile("/Users/anoop/Downloads/2015_07_22_mktplace_shop_web_log_sample.log")

    val logRegex = """^(\S+) (\S+) (\S+):(\S+) (\S+):(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+) (\S+) (\S+)" "(.*?)" (\S+) (\S+)$""".r;

    import spark.implicits._

    // Method to validate the conformance of the schema 
    def parseLog(line: String): logSchema = {
      val logRegex(timestamp, elb_name, request_ip, request_port, backend_ip, backend_port, request_processing_time, backend_processing_time, client_response_time, elb_response_code, backend_response_code, received_bytes, sent_bytes, request_verb, url, protocol, user_agent, ssl_cipher, ssl_protocol) = line;
      return logSchema(timestamp, elb_name, request_ip, request_port.toInt, backend_ip, backend_port.toInt, request_processing_time.toDouble, backend_processing_time.toDouble, client_response_time.toDouble, elb_response_code, backend_response_code, BigInt(received_bytes), BigInt(sent_bytes), request_verb, url, protocol, user_agent, ssl_cipher, ssl_protocol);
    }

    //check for the matches and then apply the schema/case class. Accomplished using  simple use of Regex match and then a map function
    // This is a standard re-usable function 
    // Here RDD API is  specifically selected for construction as log/input  is unstructured and once structure is imparted converted to DF for easy and more performant transformations
    val logs = logFile.filter(line => line.matches(logRegex.toString)).map(line => parseLog(line)).toDF

    logs.createOrReplaceTempView("logs") // The standard raw data ,structured and transformed to further usage
    logs.printSchema // to ensure all required fileds are available-  OK

    /*Starting filtering , clean up and transformation as per the business requirement
			 * 
			 * Prior to all business transformation , data needs to be filtered, cleaned up which will improve the performanace while operating on a reduced data set
       *Two things are accomplished in the below clean_and _filtered df.
       *  1.Filtered only those with a successfull response code . Please note even though this is not explicitely mentioned in the requirement, the purpose is to demonstrate the need and importance of clean up. If not required or to be changes, it is just a matter of modifiing the where condition
       *  2. Transforming the timestamp to a suitable unix_timestamp format for the ease of calculations
			 * 
			 */

    val clean_and_filtered = spark.sql(""" select distinct request_ip,url,unix_timestamp(SUBSTRING(timestamp,0,23),"yyyy-MM-dd'T'HH:mm:ss.SSS") as time_stamp from logs where backend_response_code="200" order by request_ip,time_stamp""")

    /*
			 * Now we have data ready for analysis or processing (corresponding to the SILVER zone)
			 * The  requested analysis is mainly centered around the user (identified by IP in this case) which can be achieved by grouping/partitioning based on ip but the key missing part is the session information for each user
			 * The Key logic then is to attach the session id / recreate the session which was accomplished using the below steps
			 * STEP 1 : Identify it is a new session or an existing session : key => Time difference between two immediate entry is greater than the session time out threshold. 15 minutes in this case
			 * */
    import org.apache.spark.sql.functions._

    import org.apache.spark.sql.expressions.Window

    val window = Window.partitionBy("request_ip").orderBy("time_stamp")

    // Flagged data frame implies that we decide whether it is a new session or existing session and flag it 
    val flagged_df = clean_and_filtered.withColumn("session_flag", when((lead($"time_stamp", 1, 0).over(window) - $"time_stamp") < 900, 0).otherwise(1))

    // Final DF where we assigned the session ID . Muliple Options evaluated here , use of accumulator based session generator but the rolling sum is the one that worked
    //Based on the flag (0/1) it iterates for the partition and do the sum
    val sessionized_df = flagged_df.withColumn("session_id", sum($"session_flag").over(Window.partitionBy("request_ip").orderBy("request_ip", "time_stamp")))

    sessionized_df.createOrReplaceTempView("sessionized_df") //final_df

    println(sessionized_df.count)

    /**
     *
     * ****************************ANALYTICS *******************************************
     *
     *
     */

    /*
			 * 
			 * 1. Sessionize = GrouppBy Visitor/IP
			 * 
			 */
    spark.sql("select request_ip,session_id,url from sessionized_df group by request_ip,session_id,url order by request_ip,session_id desc").show(100, false)

    /*
			 * 2.Avg Session time
			 * 
			 *  
			 */
    spark.sql("select request_ip,(max(time_stamp)-min(time_stamp))/60/(count (distinct session_id)) as avg_session_time from sessionized_df group by request_ip order by avg_session_time desc").show

    /*
			 * 3.unique url per session
			 */
    spark.sql("select request_ip,session_id,count(distinct url) from sessionized_df group by request_ip,session_id").show

    //4. IPs with longest session time 
    spark.sql("select request_ip,(max(time_stamp)-min(time_stamp)/60) as max_session_time from sessionized_df group by request_ip order by max_session_time desc").show

    spark.stop()

  }

}