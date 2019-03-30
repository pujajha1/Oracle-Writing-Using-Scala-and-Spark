// Here sc is SparkContext Object


val rddd=sc.parallelize(Array(1,2,3))
val dff=rddd.toDF("id")


val props = new java.util.Properties
props.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
props.setProperty("user", "user_name")
props.setProperty("password", "some_password")
val url: String = "jdbc:oracle:thin:@//hostname:port_number/SID"

//destination database table
val table = "some_table_name"

//In case you wish to overwrite every time :
dff.write.mode("overwrite").jdbc(url, table, props)


//In case you want to append each time, you use this:

dff.write.mode("append").jdbc(url, table, props)
