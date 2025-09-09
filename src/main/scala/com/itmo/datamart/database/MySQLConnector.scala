package com.itmo.datamart.database

import com.itmo.datamart.config.MySQLConfig
import com.itmo.datamart.model.RawFoodData
import com.typesafe.scalalogging.LazyLogging

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.time.LocalDateTime
import scala.util.{Try, Using}

class MySQLConnector(config: MySQLConfig) extends LazyLogging {
  
  // Load MySQL JDBC driver
  Class.forName("com.mysql.cj.jdbc.Driver")
  
  private def getConnection: Connection = {
    DriverManager.getConnection(
      config.jdbcUrl,
      config.user,
      config.password
    )
  }
  
  def testConnection(): Try[Boolean] = {
    Try {
      Using(getConnection) { conn =>
        val stmt = conn.prepareStatement("SELECT 1")
        val rs = stmt.executeQuery()
        rs.next() && rs.getInt(1) == 1
      }.get
    }
  }
  
  def getRawFoodData(limit: Option[Int] = None): Try[List[RawFoodData]] = {
    Try {
      val sql = s"""
        SELECT 
          id, product_name, brands, categories,
          energy_100g, proteins_100g, carbohydrates_100g,
          sugars_100g, fat_100g, saturated_fat_100g,
          fiber_100g, salt_100g, sodium_100g, created_at
        FROM processed_food_data
        ORDER BY created_at DESC
        ${limit.map(l => s"LIMIT $l").getOrElse("")}
      """
      
      Using(getConnection) { conn =>
        Using(conn.prepareStatement(sql)) { stmt =>
          Using(stmt.executeQuery()) { rs =>
            val results = scala.collection.mutable.ListBuffer[RawFoodData]()
            
            while (rs.next()) {
              val data = RawFoodData(
                id = rs.getLong("id"),
                productName = Option(rs.getString("product_name")),
                brands = Option(rs.getString("brands")),
                categories = Option(rs.getString("categories")),
                energy100g = Option(rs.getDouble("energy_100g")).filter(_ != 0.0),
                proteins100g = Option(rs.getDouble("proteins_100g")).filter(_ != 0.0),
                carbohydrates100g = Option(rs.getDouble("carbohydrates_100g")).filter(_ != 0.0),
                sugars100g = Option(rs.getDouble("sugars_100g")).filter(_ != 0.0),
                fat100g = Option(rs.getDouble("fat_100g")).filter(_ != 0.0),
                saturatedFat100g = Option(rs.getDouble("saturated_fat_100g")).filter(_ != 0.0),
                fiber100g = Option(rs.getDouble("fiber_100g")).filter(_ != 0.0),
                salt100g = Option(rs.getDouble("salt_100g")).filter(_ != 0.0),
                sodium100g = Option(rs.getDouble("sodium_100g")).filter(_ != 0.0),
                createdAt = rs.getTimestamp("created_at").toLocalDateTime
              )
              results += data
            }
            
            results.toList
          }.get
        }.get
      }.get
    }
  }
  
  def getRecordCount(): Try[Long] = {
    Try {
      val sql = "SELECT COUNT(*) as count FROM processed_food_data"
      
      Using(getConnection) { conn =>
        Using(conn.prepareStatement(sql)) { stmt =>
          Using(stmt.executeQuery()) { rs =>
            if (rs.next()) rs.getLong("count") else 0L
          }.get
        }.get
      }.get
    }
  }
  
  def getRawFoodDataBatch(offset: Int, batchSize: Int): Try[List[RawFoodData]] = {
    Try {
      val sql = s"""
        SELECT 
          id, product_name, brands, categories,
          energy_100g, proteins_100g, carbohydrates_100g,
          sugars_100g, fat_100g, saturated_fat_100g,
          fiber_100g, salt_100g, sodium_100g, created_at
        FROM processed_food_data
        ORDER BY id
        LIMIT $batchSize OFFSET $offset
      """
      
      Using(getConnection) { conn =>
        Using(conn.prepareStatement(sql)) { stmt =>
          Using(stmt.executeQuery()) { rs =>
            val results = scala.collection.mutable.ListBuffer[RawFoodData]()
            
            while (rs.next()) {
              val data = RawFoodData(
                id = rs.getLong("id"),
                productName = Option(rs.getString("product_name")),
                brands = Option(rs.getString("brands")),
                categories = Option(rs.getString("categories")),
                energy100g = getNullableDouble(rs, "energy_100g"),
                proteins100g = getNullableDouble(rs, "proteins_100g"),
                carbohydrates100g = getNullableDouble(rs, "carbohydrates_100g"),
                sugars100g = getNullableDouble(rs, "sugars_100g"),
                fat100g = getNullableDouble(rs, "fat_100g"),
                saturatedFat100g = getNullableDouble(rs, "saturated_fat_100g"),
                fiber100g = getNullableDouble(rs, "fiber_100g"),
                salt100g = getNullableDouble(rs, "salt_100g"),
                sodium100g = getNullableDouble(rs, "sodium_100g"),
                createdAt = rs.getTimestamp("created_at").toLocalDateTime
              )
              results += data
            }
            
            results.toList
          }.get
        }.get
      }.get
    }
  }
  
  private def getNullableDouble(rs: ResultSet, columnName: String): Option[Double] = {
    val value = rs.getDouble(columnName)
    if (rs.wasNull() || value == 0.0) None else Some(value)
  }
  
  def getLatestModelRuns(limit: Int = 10): Try[List[Long]] = {
    Try {
      val sql = s"""
        SELECT id FROM model_runs 
        WHERE status = 'completed'
        ORDER BY completed_at DESC 
        LIMIT $limit
      """
      
      Using(getConnection) { conn =>
        Using(conn.prepareStatement(sql)) { stmt =>
          Using(stmt.executeQuery()) { rs =>
            val results = scala.collection.mutable.ListBuffer[Long]()
            while (rs.next()) {
              results += rs.getLong("id")
            }
            results.toList
          }.get
        }.get
      }.get
    }
  }
  
  def close(): Unit = {
    logger.info("MySQL connector closed")
  }
}
