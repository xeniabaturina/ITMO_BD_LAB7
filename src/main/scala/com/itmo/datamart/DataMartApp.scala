package com.itmo.datamart

import com.itmo.datamart.api.DataMartAPI
import com.itmo.datamart.config.Config
import com.itmo.datamart.service.DataMartService
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}

object DataMartApp extends LazyLogging {
  
  def main(args: Array[String]): Unit = {
    println("=== DATAMART STARTING ===")
    logger.info("=" * 80)
    logger.info("ITMO Big Data Lab 7: ClickHouse Data Mart for Food Clustering")
    logger.info("=" * 80)
    
    try {
      println("Loading configuration...")
      // Load configuration
      val config = Config.load()
      println("Configuration loaded successfully")
      logger.info("Configuration loaded successfully")
      
      // Initialize data mart service
      println("Creating DataMart service...")
      val dataMartService = new DataMartService(config)
      
      // Initialize the service
      println("Initializing DataMart service...")
      dataMartService.initialize() match {
        case Success(_) =>
          println("DataMart service initialized successfully")
          logger.info("DataMart service initialized successfully")
        case Failure(ex) =>
          println(s"FAILED TO INITIALIZE DATAMART SERVICE: ${ex.getMessage}")
          ex.printStackTrace()
          logger.error(s"Failed to initialize DataMart service: ${ex.getMessage}", ex)
          System.exit(1)
      }
      
      // Start API server
      println("Creating DataMart API...")
      val api = new DataMartAPI(config, dataMartService)
      println("Starting DataMart API...")
      api.start() match {
        case Success(_) =>
          println("DataMart API started successfully")
          logger.info("DataMart API started successfully")
        case Failure(ex) =>
          println(s"FAILED TO START DATAMART API: ${ex.getMessage}")
          ex.printStackTrace()
          logger.error(s"Failed to start DataMart API: ${ex.getMessage}", ex)
          dataMartService.close()
          System.exit(1)
      }
      
      // Parse command line arguments
      val command = if (args.nonEmpty) args(0) else "server"
      
      command match {
        case "server" =>
          startServerMode(dataMartService, api)
          
        case "sync" =>
          runSyncMode(dataMartService, api)
          
        case "status" =>
          runStatusMode(dataMartService, api)
          
        case "help" | "--help" | "-h" =>
          printUsage()
          
        case _ =>
          logger.error(s"Unknown command: $command")
          printUsage()
          System.exit(1)
      }
      
    } catch {
      case ex: Exception =>
        // Print to stdout directly in case logging is not working
        println(s"APPLICATION FAILED TO START: ${ex.getMessage}")
        ex.printStackTrace()
        logger.error(s"Application failed to start: ${ex.getMessage}", ex)
        System.exit(1)
    }
  }
  
  private def startServerMode(dataMartService: DataMartService, api: DataMartAPI): Unit = {
    logger.info("Starting in server mode...")
    logger.info("DataMart is running and ready to serve requests")
    logger.info("Press Ctrl+C to stop the server")
    
    // Add shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Shutting down DataMart...")
      api.stop()
      dataMartService.close()
      logger.info("DataMart shutdown complete")
    }))
    
    // Keep the main thread alive
    try {
      Thread.currentThread().join()
    } catch {
      case _: InterruptedException =>
        logger.info("Application interrupted")
    }
  }
  
  private def runSyncMode(dataMartService: DataMartService, api: DataMartAPI): Unit = {
    logger.info("Running in sync mode...")
    
    try {
      dataMartService.syncDataFromMySQL() match {
        case Success(stats) =>
          logger.info("Data synchronization completed successfully")
          logger.info(s"Processing statistics: $stats")
          
        case Failure(ex) =>
          logger.error(s"Data synchronization failed: ${ex.getMessage}", ex)
          System.exit(1)
      }
    } finally {
      api.stop()
      dataMartService.close()
    }
  }
  
  private def runStatusMode(dataMartService: DataMartService, api: DataMartAPI): Unit = {
    logger.info("Checking DataMart status...")
    
    try {
      dataMartService.getDataMartStatus() match {
        case Success(status) =>
          logger.info("DataMart Status:")
          logger.info(s"  MySQL Connected: ${status.mysqlConnected}")
          logger.info(s"  ClickHouse Connected: ${status.clickhouseConnected}")
          logger.info(s"  MySQL Record Count: ${status.mysqlRecordCount}")
          logger.info(s"  ClickHouse Record Count: ${status.clickhouseRecordCount}")
          logger.info(s"  Last Sync Time: ${status.lastSyncTime}")
          
        case Failure(ex) =>
          logger.error(s"Failed to get status: ${ex.getMessage}", ex)
          System.exit(1)
      }
    } finally {
      api.stop()
      dataMartService.close()
    }
  }
  
  private def printUsage(): Unit = {
    println()
    println("ITMO Big Data Lab 7 - ClickHouse Data Mart")
    println("==========================================")
    println()
    println("Usage: java -jar food-clustering-datamart.jar [COMMAND]")
    println()
    println("Commands:")
    println("  server    Start the DataMart API server (default)")
    println("  sync      Sync data from MySQL to ClickHouse and exit")
    println("  status    Check DataMart status and exit")
    println("  help      Show this help message")
    println()
    println("Examples:")
    println("  java -jar food-clustering-datamart.jar server")
    println("  java -jar food-clustering-datamart.jar sync")
    println("  java -jar food-clustering-datamart.jar status")
    println()
    println("API Endpoints (when running in server mode):")
    println("  GET  http://localhost:8080/health   - Health check")
    println("  GET  http://localhost:8080/status   - DataMart status")
    println("  POST http://localhost:8080/sync     - Trigger data sync")
    println("  POST http://localhost:8080/data     - Get processed data")
    println("  POST http://localhost:8080/results  - Store model results")
    println()
    println("Configuration:")
    println("  Edit src/main/resources/application.conf to customize settings")
    println("  Environment variables can override configuration values")
    println()
  }
} 