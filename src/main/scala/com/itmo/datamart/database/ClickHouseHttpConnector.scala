package com.itmo.datamart.database

import com.itmo.datamart.config.ClickHouseConfig
import com.itmo.datamart.model._
import com.typesafe.scalalogging.LazyLogging
import sttp.client3._
import sttp.client3.circe._
import io.circe.generic.auto._
import io.circe.parser._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Try, Success, Failure}

class ClickHouseHttpConnector(config: ClickHouseConfig) extends LazyLogging {
  
  private val backend = HttpURLConnectionBackend()
  private val baseUrl = s"http://${config.host}:${config.port}"
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  
  def testConnection(): Try[Boolean] = {
    Try {
      println(s"DEBUG: Testing HTTP connection to $baseUrl")
      val request = basicRequest
        .get(uri"$baseUrl")
        .auth.basic(config.user, config.password)
        .readTimeout(scala.concurrent.duration.Duration(config.connectionTimeout, "ms"))
      
      val response = request.send(backend)
      response.code.code == 200 && response.body.contains("Ok")
    }
  }
  
  def initializeSchema(): Try[Unit] = {
    Try {
      val statements = List(
        // Create database if not exists
        s"CREATE DATABASE IF NOT EXISTS ${config.database}",
        
        // Processed food data table
        s"""
        CREATE TABLE IF NOT EXISTS ${config.database}.processed_food_data (
          id UInt64,
          product_name String,
          brands String,
          categories String,
          energy_100g Float64,
          proteins_100g Float64,
          carbohydrates_100g Float64,
          sugars_100g Float64,
          fat_100g Float64,
          saturated_fat_100g Float64,
          fiber_100g Float64,
          salt_100g Float64,
          sodium_100g Float64,
          processed_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (id, processed_at)
        """,
        
        // Feature vectors table
        s"""
        CREATE TABLE IF NOT EXISTS ${config.database}.feature_vectors (
          id UInt64,
          features Array(Float64),
          created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (id, created_at)
        """,
        
        // Model runs table
        s"""
        CREATE TABLE IF NOT EXISTS ${config.database}.model_runs (
          run_id UInt64,
          run_name String,
          silhouette_score Float64,
          status String,
          started_at DateTime,
          completed_at Nullable(DateTime)
        ) ENGINE = MergeTree()
        ORDER BY (run_id, started_at)
        """,
        
        // Clustering results table
        s"""
        CREATE TABLE IF NOT EXISTS ${config.database}.clustering_results (
          id UInt64,
          model_run_id UInt64,
          processed_data_id UInt64,
          cluster_id UInt32,
          distance_to_center Float64,
          created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (model_run_id, cluster_id, id)
        """,
        
        // Cluster centers table
        s"""
        CREATE TABLE IF NOT EXISTS ${config.database}.cluster_centers (
          id UInt64,
          model_run_id UInt64,
          cluster_id UInt32,
          energy_100g Float64,
          proteins_100g Float64,
          carbohydrates_100g Float64,
          sugars_100g Float64,
          fat_100g Float64,
          saturated_fat_100g Float64,
          fiber_100g Float64,
          salt_100g Float64,
          sodium_100g Float64,
          created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (model_run_id, cluster_id)
        """
      )
      
      statements.foreach { sql =>
        executeQuery(sql) match {
          case Success(_) => 
            logger.debug(s"Executed schema statement successfully")
          case Failure(ex) => 
            logger.error(s"Failed to execute schema statement: ${ex.getMessage}")
            throw ex
        }
      }
      
      logger.info("ClickHouse schema initialized successfully via HTTP")
    }
  }
  
  private def executeQuery(sql: String): Try[String] = {
    Try {
      val request = basicRequest
        .post(uri"$baseUrl")
        .auth.basic(config.user, config.password)
        .body(sql)
        .readTimeout(scala.concurrent.duration.Duration(config.socketTimeout, "ms"))
      
      val response = request.send(backend)
      
      if (response.code.code == 200) {
        response.body match {
          case Right(body) => body
          case Left(error) => throw new RuntimeException(s"ClickHouse query failed: $error")
        }
      } else {
        throw new RuntimeException(s"ClickHouse HTTP error: ${response.code} - ${response.body}")
      }
    }
  }
  
  def insertProcessedData(data: List[ProcessedFoodData]): Try[Int] = {
    Try {
      if (data.isEmpty) return Try(0)
      
      val values = data.map { item =>
        s"(${item.id}, '${escapeString(item.productName)}', '${escapeString(item.brands)}', " +
        s"'${escapeString(item.categories)}', ${item.energy100g}, ${item.proteins100g}, " +
        s"${item.carbohydrates100g}, ${item.sugars100g}, ${item.fat100g}, ${item.saturatedFat100g}, " +
        s"${item.fiber100g}, ${item.salt100g}, ${item.sodium100g}, " +
        s"'${dateFormatter.format(item.processedAt)}')"
      }.mkString(", ")
      
      val sql = s"""
        INSERT INTO ${config.database}.processed_food_data 
        (id, product_name, brands, categories, energy_100g, proteins_100g, 
         carbohydrates_100g, sugars_100g, fat_100g, saturated_fat_100g, 
         fiber_100g, salt_100g, sodium_100g, processed_at)
        VALUES $values
      """
      
      executeQuery(sql) match {
        case Success(_) => data.length
        case Failure(ex) => throw ex
      }
    }
  }
  
  def insertFeatureVectors(vectors: List[FeatureVector]): Try[Int] = {
    Try {
      if (vectors.isEmpty) return Try(0)
      
      val values = vectors.map { vector =>
        val featuresArray = "[" + vector.features.mkString(", ") + "]"
        s"(${vector.id}, $featuresArray, '${dateFormatter.format(LocalDateTime.now())}')"
      }.mkString(", ")
      
      val sql = s"""
        INSERT INTO ${config.database}.feature_vectors 
        (id, features, created_at)
        VALUES $values
      """
      
      executeQuery(sql) match {
        case Success(_) => vectors.length
        case Failure(ex) => throw ex
      }
    }
  }
  
  def getFeatureVectors(limit: Option[Int] = None): Try[List[FeatureVector]] = {
    Try {
      val limitClause = limit.map(l => s"LIMIT $l").getOrElse("")
      val sql = s"""
        SELECT id, features 
        FROM ${config.database}.feature_vectors 
        ORDER BY created_at DESC
        $limitClause
        FORMAT JSONEachRow
      """
      
      executeQuery(sql) match {
        case Success(jsonResponse) =>
          // Parse JSON response - each line is a JSON object
          val lines = jsonResponse.split("\n").filter(_.nonEmpty)
          lines.map { line =>
            parse(line) match {
              case Right(json) =>
                val cursor = json.hcursor
                val id = cursor.get[Long]("id").getOrElse(0L)
                val features = cursor.get[List[Double]]("features").getOrElse(List.empty)
                FeatureVector(id, features.toArray)
              case Left(error) =>
                logger.error(s"Failed to parse JSON: $error")
                throw new RuntimeException(s"JSON parsing error: $error")
            }
          }.toList
          
        case Failure(ex) => throw ex
      }
    }
  }
  
  def insertModelRun(modelRun: ModelRunResult): Try[Unit] = {
    Try {
      val completedAtStr = modelRun.completedAt
        .map(dt => s"'${dateFormatter.format(dt)}'")
        .getOrElse("NULL")
      
      val sql = s"""
        INSERT INTO ${config.database}.model_runs 
        (run_id, run_name, silhouette_score, status, started_at, completed_at)
        VALUES (${modelRun.runId}, '${escapeString(modelRun.runName)}', 
               ${modelRun.silhouetteScore}, '${escapeString(modelRun.status)}', 
               '${dateFormatter.format(modelRun.startedAt)}', $completedAtStr)
      """
      
      executeQuery(sql) match {
        case Success(_) => ()
        case Failure(ex) => throw ex
      }
    }
  }
  
  def insertClusteringResults(results: List[ClusteringResult]): Try[Int] = {
    Try {
      if (results.isEmpty) return Try(0)
      
      val values = results.map { result =>
        s"(${result.id}, ${result.modelRunId}, ${result.processedDataId}, " +
        s"${result.clusterId}, ${result.distanceToCenter}, " +
        s"'${dateFormatter.format(LocalDateTime.now())}')"
      }.mkString(", ")
      
      val sql = s"""
        INSERT INTO ${config.database}.clustering_results 
        (id, model_run_id, processed_data_id, cluster_id, distance_to_center, created_at)
        VALUES $values
      """
      
      executeQuery(sql) match {
        case Success(_) => results.length
        case Failure(ex) => throw ex
      }
    }
  }
  
  def insertClusterCenters(centers: List[ClusterCenter]): Try[Int] = {
    Try {
      if (centers.isEmpty) return Try(0)
      
      val values = centers.map { center =>
        s"(${center.id}, ${center.modelRunId}, ${center.clusterId}, " +
        s"${center.energy100g}, ${center.proteins100g}, ${center.carbohydrates100g}, " +
        s"${center.sugars100g}, ${center.fat100g}, ${center.saturatedFat100g}, " +
        s"${center.fiber100g}, ${center.salt100g}, ${center.sodium100g}, " +
        s"'${dateFormatter.format(LocalDateTime.now())}')"
      }.mkString(", ")
      
      val sql = s"""
        INSERT INTO ${config.database}.cluster_centers 
        (id, model_run_id, cluster_id, energy_100g, proteins_100g, carbohydrates_100g,
         sugars_100g, fat_100g, saturated_fat_100g, fiber_100g, salt_100g, sodium_100g, created_at)
        VALUES $values
      """
      
      executeQuery(sql) match {
        case Success(_) => centers.length
        case Failure(ex) => throw ex
      }
    }
  }
  
  def getRecordCount(): Try[Long] = {
    Try {
      val sql = s"SELECT count() FROM ${config.database}.processed_food_data FORMAT JSONEachRow"
      
      executeQuery(sql) match {
        case Success(jsonResponse) =>
          parse(jsonResponse.split("\n").head) match {
            case Right(json) =>
              json.hcursor.get[Long]("count()").getOrElse(0L)
            case Left(_) => 0L
          }
        case Failure(_) => 0L
      }
    }
  }
  
  private def escapeString(str: String): String = {
    if (str == null || str.isEmpty) return ""
    
    // Clean and escape the string for ClickHouse
    str
      .replace("\\", "\\\\")  // Escape backslashes first
      .replace("'", "\\'")    // Escape single quotes
      .replace("\"", "\\\"")  // Escape double quotes
      .replace("\n", "\\n")   // Escape newlines
      .replace("\r", "\\r")   // Escape carriage returns
      .replace("\t", "\\t")   // Escape tabs
      .filter(c => c >= 32 && c < 127 || c == ' ') // Keep only printable ASCII and space
      .take(500)              // Limit length to prevent issues
  }
  
  def close(): Unit = {
    backend.close()
  }
} 