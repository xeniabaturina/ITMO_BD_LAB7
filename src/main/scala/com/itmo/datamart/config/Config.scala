package com.itmo.datamart.config

import com.typesafe.config.ConfigFactory
import scala.jdk.CollectionConverters._

case class ClickHouseConfig(
  host: String,
  port: Int,
  database: String,
  user: String,
  password: String,
  maxConnections: Int,
  connectionTimeout: Int,
  socketTimeout: Int
) {
  def jdbcUrl: String = s"jdbc:clickhouse://$host:$port/$database"
}

case class MySQLConfig(
  host: String,
  port: Int,
  database: String,
  user: String,
  password: String,
  maxConnections: Int,
  connectionTimeout: Int
) {
  def jdbcUrl: String = s"jdbc:mysql://$host:$port/$database?useSSL=false&allowPublicKeyRetrieval=true"
}

case class ApiConfig(
  host: String,
  port: Int,
  requestTimeout: Int
)

case class ProcessingConfig(
  batchSize: Int,
  features: List[String],
  outlierPercentiles: List[Double],
  minValidFields: Int,
  maxRecords: Int
)

case class LoggingConfig(
  level: String,
  pattern: String
)

case class DataMartConfig(
  clickhouse: ClickHouseConfig,
  mysql: MySQLConfig,
  api: ApiConfig,
  processing: ProcessingConfig,
  logging: LoggingConfig
)

object Config {
  private val config = ConfigFactory.load()
  
  def load(): DataMartConfig = {
    // Debug: print environment variables
    println(s"DEBUG: CLICKHOUSE_HOST = ${sys.env.get("CLICKHOUSE_HOST")}")
    println(s"DEBUG: MYSQL_HOST = ${sys.env.get("MYSQL_HOST")}")
    
    val datamartConfig = config.getConfig("datamart")
    
    val clickhouseConfig = datamartConfig.getConfig("clickhouse")
    val mysqlConfig = datamartConfig.getConfig("mysql")
    val apiConfig = datamartConfig.getConfig("api")
    val processingConfig = datamartConfig.getConfig("processing")
    val loggingConfig = datamartConfig.getConfig("logging")
    
    // Debug: print what config values are being loaded
    val clickhouseHost = clickhouseConfig.getString("host")
    val mysqlHost = mysqlConfig.getString("host")
    println(s"DEBUG: Loaded ClickHouse host: $clickhouseHost")
    println(s"DEBUG: Loaded MySQL host: $mysqlHost")
    
    val clickhousePort = clickhouseConfig.getInt("port")
    val clickhouseDatabase = clickhouseConfig.getString("database")
    val clickhouseJdbcUrl = s"jdbc:clickhouse://$clickhouseHost:$clickhousePort/$clickhouseDatabase"
    println(s"DEBUG: ClickHouse JDBC URL: $clickhouseJdbcUrl")
    
    DataMartConfig(
      clickhouse = ClickHouseConfig(
        host = clickhouseHost,
        port = clickhousePort,
        database = clickhouseDatabase,
        user = clickhouseConfig.getString("user"),
        password = clickhouseConfig.getString("password"),
        maxConnections = clickhouseConfig.getInt("max_connections"),
        connectionTimeout = clickhouseConfig.getInt("connection_timeout"),
        socketTimeout = clickhouseConfig.getInt("socket_timeout")
      ),
      mysql = MySQLConfig(
        host = mysqlHost,
        port = mysqlConfig.getInt("port"),
        database = mysqlConfig.getString("database"),
        user = mysqlConfig.getString("user"),
        password = mysqlConfig.getString("password"),
        maxConnections = mysqlConfig.getInt("max_connections"),
        connectionTimeout = mysqlConfig.getInt("connection_timeout")
      ),
      api = ApiConfig(
        host = apiConfig.getString("host"),
        port = apiConfig.getInt("port"),
        requestTimeout = apiConfig.getInt("request_timeout")
      ),
      processing = ProcessingConfig(
        batchSize = processingConfig.getInt("batch_size"),
        features = processingConfig.getStringList("features").asScala.toList,
        outlierPercentiles = processingConfig.getDoubleList("outlier_percentiles").asScala.map(_.doubleValue()).toList,
        minValidFields = processingConfig.getInt("min_valid_fields"),
        maxRecords = processingConfig.getInt("max_records")
      ),
      logging = LoggingConfig(
        level = loggingConfig.getString("level"),
        pattern = loggingConfig.getString("pattern")
      )
    )
  }
}
