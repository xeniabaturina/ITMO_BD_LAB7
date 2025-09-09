package com.itmo.datamart.api

import com.itmo.datamart.config.DataMartConfig
import com.itmo.datamart.model._
import com.itmo.datamart.model.FoodData._
import com.itmo.datamart.service.{DataMartService, DataMartStatus}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._

import java.io.{BufferedReader, InputStreamReader, OutputStream}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

// Response case classes for JSON serialization
case class HealthResponse(status: String, timestamp: Long)
case class SyncResponse(success: Boolean, message: String, stats: ProcessingStats)
case class ErrorResponse(success: Boolean, error: String)

class DataMartAPI(config: DataMartConfig, dataMartService: DataMartService) extends LazyLogging {
  
  private var server: Option[HttpServer] = None
  
  def start(): Try[Unit] = {
    Try {
      val httpServer = HttpServer.create(new InetSocketAddress(config.api.host, config.api.port), 0)
      
      // Register endpoints
      httpServer.createContext("/health", new HealthHandler)
      httpServer.createContext("/status", new StatusHandler)
      httpServer.createContext("/sync", new SyncHandler)
      httpServer.createContext("/data", new DataHandler)
      httpServer.createContext("/results", new ResultsHandler)
      
      httpServer.setExecutor(null) // Use default executor
      httpServer.start()
      
      server = Some(httpServer)
      
      logger.info(s"DataMart API server started on ${config.api.host}:${config.api.port}")
      logger.info("Available endpoints:")
      logger.info("  GET  /health  - Health check")
      logger.info("  GET  /status  - DataMart status")
      logger.info("  POST /sync    - Sync data from MySQL")
      logger.info("  POST /data    - Get processed data for ML model")
      logger.info("  POST /results - Store ML model results")
    }
  }
  
  def stop(): Unit = {
    server.foreach { s =>
      s.stop(0)
      logger.info("DataMart API server stopped")
    }
    server = None
  }
  
  private def sendResponse(exchange: HttpExchange, statusCode: Int, response: String): Unit = {
    val responseBytes = response.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
    exchange.sendResponseHeaders(statusCode, responseBytes.length)
    
    val outputStream = exchange.getResponseBody
    outputStream.write(responseBytes)
    outputStream.close()
  }
  
  private def readRequestBody(exchange: HttpExchange): String = {
    val inputStream = exchange.getRequestBody
    val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    val requestBody = new StringBuilder
    
    var line = reader.readLine()
    while (line != null) {
      requestBody.append(line)
      line = reader.readLine()
    }
    
    reader.close()
    requestBody.toString
  }
  
  private class HealthHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        if (exchange.getRequestMethod == "GET") {
          val healthResponse = HealthResponse("healthy", System.currentTimeMillis())
          val response = healthResponse.asJson.noSpaces
          sendResponse(exchange, 200, response)
        } else {
          sendResponse(exchange, 405, """{"error": "Method not allowed"}""")
        }
      } catch {
        case ex: Exception =>
          logger.error(s"Health check failed: ${ex.getMessage}", ex)
          sendResponse(exchange, 500, s"""{"error": "${ex.getMessage}"}""")
      }
    }
  }
  
  private class StatusHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        if (exchange.getRequestMethod == "GET") {
          dataMartService.getDataMartStatus() match {
            case Success(status) =>
              val response = status.asJson.noSpaces
              sendResponse(exchange, 200, response)
            case Failure(ex) =>
              logger.error(s"Failed to get status: ${ex.getMessage}", ex)
              sendResponse(exchange, 500, s"""{"error": "${ex.getMessage}"}""")
          }
        } else {
          sendResponse(exchange, 405, """{"error": "Method not allowed"}""")
        }
      } catch {
        case ex: Exception =>
          logger.error(s"Status check failed: ${ex.getMessage}", ex)
          sendResponse(exchange, 500, s"""{"error": "${ex.getMessage}"}""")
      }
    }
  }
  
  private class SyncHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        if (exchange.getRequestMethod == "POST") {
          logger.info("Received sync request")
          
          dataMartService.syncDataFromMySQL() match {
            case Success(stats) =>
              val syncResponse = SyncResponse(
                success = true,
                message = "Data synchronization completed",
                stats = stats
              )
              val response = syncResponse.asJson.noSpaces
              sendResponse(exchange, 200, response)
              
            case Failure(ex) =>
              logger.error(s"Data sync failed: ${ex.getMessage}", ex)
              val errorResponse = ErrorResponse(
                success = false,
                error = ex.getMessage
              )
              val response = errorResponse.asJson.noSpaces
              sendResponse(exchange, 500, response)
          }
        } else {
          sendResponse(exchange, 405, """{"error": "Method not allowed"}""")
        }
      } catch {
        case ex: Exception =>
          logger.error(s"Sync request failed: ${ex.getMessage}", ex)
          sendResponse(exchange, 500, s"""{"error": "${ex.getMessage}"}""")
      }
    }
  }
  
  private class DataHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        if (exchange.getRequestMethod == "POST") {
          val requestBody = readRequestBody(exchange)
          logger.info(s"Received data request: $requestBody")
          
          // Parse request
          decode[DataRequest](requestBody) match {
            case Right(dataRequest) =>
              dataMartService.getProcessedData(dataRequest) match {
                case Success(dataResponse) =>
                  val response = dataResponse.asJson.noSpaces
                  sendResponse(exchange, 200, response)
                  
                case Failure(ex) =>
                  logger.error(s"Failed to get processed data: ${ex.getMessage}", ex)
                  val errorResponse = ErrorResponse(
                    success = false,
                    error = ex.getMessage
                  )
                  val response = errorResponse.asJson.noSpaces
                  sendResponse(exchange, 500, response)
              }
              
            case Left(error) =>
              logger.error(s"Failed to parse data request: $error")
              sendResponse(exchange, 400, s"""{"error": "Invalid request format: $error"}""")
          }
        } else {
          sendResponse(exchange, 405, """{"error": "Method not allowed"}""")
        }
      } catch {
        case ex: Exception =>
          logger.error(s"Data request failed: ${ex.getMessage}", ex)
          sendResponse(exchange, 500, s"""{"error": "${ex.getMessage}"}""")
      }
    }
  }
  
  private class ResultsHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      try {
        if (exchange.getRequestMethod == "POST") {
          val requestBody = readRequestBody(exchange)
          logger.info(s"Received results request for ${requestBody.length} characters")
          
          // Parse request
          decode[ResultRequest](requestBody) match {
            case Right(resultRequest) =>
              dataMartService.storeModelResults(resultRequest) match {
                case Success(resultResponse) =>
                  val response = resultResponse.asJson.noSpaces
                  sendResponse(exchange, 200, response)
                  
                case Failure(ex) =>
                  logger.error(s"Failed to store model results: ${ex.getMessage}", ex)
                  val errorResponse = ErrorResponse(
                    success = false,
                    error = ex.getMessage
                  )
                  val response = errorResponse.asJson.noSpaces
                  sendResponse(exchange, 500, response)
              }
              
            case Left(error) =>
              logger.error(s"Failed to parse result request: $error")
              sendResponse(exchange, 400, s"""{"error": "Invalid request format: $error"}""")
          }
        } else {
          sendResponse(exchange, 405, """{"error": "Method not allowed"}""")
        }
      } catch {
        case ex: Exception =>
          logger.error(s"Results request failed: ${ex.getMessage}", ex)
          sendResponse(exchange, 500, s"""{"error": "${ex.getMessage}"}""")
      }
    }
  }
} 