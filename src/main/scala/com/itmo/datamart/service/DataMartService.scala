package com.itmo.datamart.service

import com.itmo.datamart.config.DataMartConfig
import com.itmo.datamart.database.{ClickHouseHttpConnector, MySQLConnector}
import com.itmo.datamart.model._
import com.itmo.datamart.processor.{DataProcessor, DataProcessingResult}
import com.typesafe.scalalogging.LazyLogging

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

class DataMartService(config: DataMartConfig) extends LazyLogging {
  
  private val mysqlConnector = new MySQLConnector(config.mysql)
  private val clickhouseConnector = new ClickHouseHttpConnector(config.clickhouse)
  private val dataProcessor = new DataProcessor(config.processing)
  
  def initialize(): Try[Unit] = {
    Try {
      logger.info("Initializing DataMart service...")
      
      // Test connections
      mysqlConnector.testConnection() match {
        case Success(_) => logger.info("MySQL connection test successful")
        case Failure(ex) => 
          logger.error(s"MySQL connection test failed: ${ex.getMessage}")
          throw ex
      }
      
      clickhouseConnector.testConnection() match {
        case Success(_) => logger.info("ClickHouse HTTP connection test successful")
        case Failure(ex) => 
          logger.error(s"ClickHouse HTTP connection test failed: ${ex.getMessage}")
          throw ex
      }
      
      // Initialize ClickHouse schema
      clickhouseConnector.initializeSchema() match {
        case Success(_) => logger.info("ClickHouse schema initialized via HTTP")
        case Failure(ex) => 
          logger.error(s"ClickHouse schema initialization failed: ${ex.getMessage}")
          throw ex
      }
      
      logger.info("DataMart service initialized successfully")
    }
  }
  
  def syncDataFromMySQL(): Try[ProcessingStats] = {
    Try {
      logger.info("Starting data synchronization from MySQL...")
      
      // Get raw data from MySQL
      val rawDataResult = mysqlConnector.getRawFoodData(Some(config.processing.maxRecords))
      
      rawDataResult match {
        case Success(rawData) =>
          logger.info(s"Retrieved ${rawData.length} raw records from MySQL")
          
          if (rawData.nonEmpty) {
            // Process data in batches
            val batchResults = dataProcessor.processBatch(rawData, config.processing.batchSize)
            
            batchResults match {
              case Success(results) =>
                // Combine all results
                val allProcessedData = results.flatMap(_.processedData)
                val allFeatureVectors = results.flatMap(_.featureVectors)
                val combinedStats = combineStats(results.map(_.stats))
                
                // Store in ClickHouse
                storeProcessedData(allProcessedData, allFeatureVectors) match {
                  case Success(_) =>
                    logger.info(s"Successfully stored ${allProcessedData.length} processed records in ClickHouse")
                    combinedStats
                  case Failure(ex) =>
                    logger.error(s"Failed to store processed data: ${ex.getMessage}")
                    throw ex
                }
                
              case Failure(ex) =>
                logger.error(s"Data processing failed: ${ex.getMessage}")
                throw ex
            }
          } else {
            logger.warn("No raw data found in MySQL")
            ProcessingStats(0, 0, 0, 0, 0)
          }
          
        case Failure(ex) =>
          logger.error(s"Failed to retrieve data from MySQL: ${ex.getMessage}")
          throw ex
      }
    }
  }
  
  private def storeProcessedData(processedData: List[ProcessedFoodData], 
                                featureVectors: List[FeatureVector]): Try[Unit] = {
    Try {
      // Store processed data
      clickhouseConnector.insertProcessedData(processedData) match {
        case Success(count) => 
          logger.info(s"Inserted $count processed data records")
        case Failure(ex) => 
          logger.error(s"Failed to insert processed data: ${ex.getMessage}")
          throw ex
      }
      
      // Store feature vectors
      clickhouseConnector.insertFeatureVectors(featureVectors) match {
        case Success(count) => 
          logger.info(s"Inserted $count feature vectors")
        case Failure(ex) => 
          logger.error(s"Failed to insert feature vectors: ${ex.getMessage}")
          throw ex
      }
    }
  }
  
  private def combineStats(statsList: List[ProcessingStats]): ProcessingStats = {
    ProcessingStats(
      totalRecords = statsList.map(_.totalRecords).sum,
      validRecords = statsList.map(_.validRecords).sum,
      invalidRecords = statsList.map(_.invalidRecords).sum,
      outliersRemoved = statsList.map(_.outliersRemoved).sum,
      processingTime = statsList.map(_.processingTime).sum
    )
  }
  
  def getProcessedData(request: DataRequest): Try[DataResponse] = {
    Try {
      logger.info(s"Retrieving processed data with request: $request")
      
      val limit = request.maxRecords.getOrElse(config.processing.maxRecords)
      
      clickhouseConnector.getFeatureVectors(Some(limit)) match {
        case Success(featureVectors) =>
          val stats = ProcessingStats(
            totalRecords = featureVectors.length,
            validRecords = featureVectors.length,
            invalidRecords = 0,
            outliersRemoved = 0,
            processingTime = 0
          )
          
          logger.info(s"Retrieved ${featureVectors.length} feature vectors from ClickHouse")
          DataResponse(featureVectors, stats)
          
        case Failure(ex) =>
          logger.error(s"Failed to retrieve feature vectors: ${ex.getMessage}")
          throw ex
      }
    }
  }
  
  def storeModelResults(request: ResultRequest): Try[ResultResponse] = {
    Try {
      logger.info(s"Storing model results for run ${request.modelRunId}")
      
      // Store model run info
      val modelRun = ModelRunResult(
        runId = request.modelRunId,
        runName = s"clustering_run_${request.modelRunId}",
        silhouetteScore = 0.0, // Will be updated later
        status = "completed",
        startedAt = LocalDateTime.now(),
        completedAt = Some(LocalDateTime.now())
      )
      
      clickhouseConnector.insertModelRun(modelRun) match {
        case Success(_) => 
          logger.info(s"Stored model run info for run ${request.modelRunId}")
        case Failure(ex) => 
          logger.error(s"Failed to store model run: ${ex.getMessage}")
          throw ex
      }
      
      // Store clustering results
      var recordsStored = 0
      
      if (request.results.nonEmpty) {
        clickhouseConnector.insertClusteringResults(request.results) match {
          case Success(count) => 
            recordsStored += count
            logger.info(s"Stored $count clustering results")
          case Failure(ex) => 
            logger.error(s"Failed to store clustering results: ${ex.getMessage}")
            throw ex
        }
      }
      
      // Store cluster centers
      if (request.clusterCenters.nonEmpty) {
        clickhouseConnector.insertClusterCenters(request.clusterCenters) match {
          case Success(count) => 
            recordsStored += count
            logger.info(s"Stored $count cluster centers")
          case Failure(ex) => 
            logger.error(s"Failed to store cluster centers: ${ex.getMessage}")
            throw ex
        }
      }
      
      ResultResponse(
        success = true,
        message = "Model results stored successfully",
        recordsStored = recordsStored
      )
    }
  }
  
  def getDataMartStatus(): Try[DataMartStatus] = {
    Try {
      val mysqlRecordCount = mysqlConnector.getRecordCount().getOrElse(0L)
      val clickhouseRecordCount = clickhouseConnector.getRecordCount().getOrElse(0L)
      
      DataMartStatus(
        mysqlConnected = mysqlConnector.testConnection().isSuccess,
        clickhouseConnected = clickhouseConnector.testConnection().isSuccess,
        mysqlRecordCount = mysqlRecordCount,
        clickhouseRecordCount = clickhouseRecordCount,
        lastSyncTime = LocalDateTime.now() // In production, store this
      )
    }
  }
  
  def processIncrementalData(): Try[ProcessingStats] = {
    Try {
      logger.info("Processing incremental data from MySQL...")
      
      // Get latest records from MySQL
      // This is a simplified version - in production you'd track last sync timestamp
      val batchSize = config.processing.batchSize
      val rawDataResult = mysqlConnector.getRawFoodDataBatch(0, batchSize)
      
      rawDataResult match {
        case Success(rawData) =>
          if (rawData.nonEmpty) {
            val processingResult = dataProcessor.processRawData(rawData)
            
            processingResult match {
              case Success(result) =>
                storeProcessedData(result.processedData, result.featureVectors) match {
                  case Success(_) =>
                    logger.info(s"Successfully processed ${result.processedData.length} incremental records")
                    result.stats
                  case Failure(ex) =>
                    logger.error(s"Failed to store incremental data: ${ex.getMessage}")
                    throw ex
                }
              case Failure(ex) =>
                logger.error(s"Failed to process incremental data: ${ex.getMessage}")
                throw ex
            }
          } else {
            logger.info("No new data to process")
            ProcessingStats(0, 0, 0, 0, 0)
          }
          
        case Failure(ex) =>
          logger.error(s"Failed to retrieve incremental data: ${ex.getMessage}")
          throw ex
      }
    }
  }
  
  def close(): Unit = {
    logger.info("Closing DataMart service...")
    mysqlConnector.close()
    clickhouseConnector.close()
    logger.info("DataMart service closed")
  }
}

case class DataMartStatus(
  mysqlConnected: Boolean,
  clickhouseConnected: Boolean,
  mysqlRecordCount: Long,
  clickhouseRecordCount: Long,
  lastSyncTime: LocalDateTime
)
