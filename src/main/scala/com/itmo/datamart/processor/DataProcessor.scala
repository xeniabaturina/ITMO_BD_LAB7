package com.itmo.datamart.processor

import com.itmo.datamart.config.ProcessingConfig
import com.itmo.datamart.model._
import com.typesafe.scalalogging.LazyLogging

import java.time.LocalDateTime
import scala.util.Try

class DataProcessor(config: ProcessingConfig) extends LazyLogging {
  
  def processRawData(rawData: List[RawFoodData]): Try[DataProcessingResult] = {
    Try {
      val startTime = System.currentTimeMillis()
      logger.info(s"Starting data processing for ${rawData.length} raw records")
      
      // Step 1: Filter valid records
      val validRecords = filterValidRecords(rawData)
      logger.info(s"Valid records after filtering: ${validRecords.length}")
      
      // Step 2: Remove outliers
      val (cleanRecords, outliersRemoved) = removeOutliers(validRecords)
      logger.info(s"Records after outlier removal: ${cleanRecords.length}, outliers removed: $outliersRemoved")
      
      // Step 3: Convert to processed data
      val processedData = cleanRecords.map(convertToProcessedData)
      
      // Step 4: Extract features
      val featureVectors = processedData.map(extractFeatures)
      
      val endTime = System.currentTimeMillis()
      val processingTime = endTime - startTime
      
      val stats = ProcessingStats(
        totalRecords = rawData.length,
        validRecords = validRecords.length,
        invalidRecords = rawData.length - validRecords.length,
        outliersRemoved = outliersRemoved,
        processingTime = processingTime
      )
      
      logger.info(s"Data processing completed in ${processingTime}ms")
      logger.info(s"Processing stats: $stats")
      
      DataProcessingResult(processedData, featureVectors, stats)
    }
  }
  
  private def filterValidRecords(rawData: List[RawFoodData]): List[RawFoodData] = {
    rawData.filter { record =>
      val nutritionalValues = List(
        record.energy100g,
        record.proteins100g,
        record.carbohydrates100g,
        record.sugars100g,
        record.fat100g,
        record.saturatedFat100g,
        record.fiber100g,
        record.salt100g,
        record.sodium100g
      )
      
      val validFieldCount = nutritionalValues.count(_.isDefined)
      val hasValidName = record.productName.exists(_.trim.nonEmpty)
      
      validFieldCount >= config.minValidFields && hasValidName
    }
  }
  
  private def removeOutliers(data: List[RawFoodData]): (List[RawFoodData], Long) = {
    if (data.isEmpty) return (data, 0L)
    
    // Calculate percentiles for each feature
    val features = config.features
    val percentiles = calculatePercentiles(data, features)
    
    // Filter out outliers
    val (cleanData, outliers) = data.partition { record =>
      features.forall { feature =>
        getFeatureValue(record, feature) match {
          case Some(value) =>
            val (lowerBound, upperBound) = percentiles(feature)
            value >= lowerBound && value <= upperBound
          case None => true // Keep records with missing values for this feature
        }
      }
    }
    
    (cleanData, outliers.length.toLong)
  }
  
  private def calculatePercentiles(data: List[RawFoodData], features: List[String]): Map[String, (Double, Double)] = {
    features.map { feature =>
      val values = data.flatMap(record => getFeatureValue(record, feature)).sorted
      
      if (values.nonEmpty) {
        val lowerIndex = (values.length * config.outlierPercentiles.head).toInt
        val upperIndex = (values.length * config.outlierPercentiles(1)).toInt
        
        val lowerBound = if (lowerIndex < values.length) values(lowerIndex) else values.head
        val upperBound = if (upperIndex < values.length) values(upperIndex) else values.last
        
        feature -> (lowerBound, upperBound)
      } else {
        feature -> (0.0, Double.MaxValue)
      }
    }.toMap
  }
  
  private def getFeatureValue(record: RawFoodData, feature: String): Option[Double] = {
    feature match {
      case "energy_100g" => record.energy100g
      case "proteins_100g" => record.proteins100g
      case "carbohydrates_100g" => record.carbohydrates100g
      case "sugars_100g" => record.sugars100g
      case "fat_100g" => record.fat100g
      case "saturated_fat_100g" => record.saturatedFat100g
      case "fiber_100g" => record.fiber100g
      case "salt_100g" => record.salt100g
      case "sodium_100g" => record.sodium100g
      case _ => None
    }
  }
  
  private def convertToProcessedData(raw: RawFoodData): ProcessedFoodData = {
    ProcessedFoodData(
      id = raw.id,
      productName = raw.productName.getOrElse("Unknown Product"),
      brands = raw.brands.getOrElse("Unknown Brand"),
      categories = raw.categories.getOrElse("Unknown Category"),
      energy100g = raw.energy100g.getOrElse(0.0),
      proteins100g = raw.proteins100g.getOrElse(0.0),
      carbohydrates100g = raw.carbohydrates100g.getOrElse(0.0),
      sugars100g = raw.sugars100g.getOrElse(0.0),
      fat100g = raw.fat100g.getOrElse(0.0),
      saturatedFat100g = raw.saturatedFat100g.getOrElse(0.0),
      fiber100g = raw.fiber100g.getOrElse(0.0),
      salt100g = raw.salt100g.getOrElse(0.0),
      sodium100g = raw.sodium100g.getOrElse(0.0),
      processedAt = LocalDateTime.now()
    )
  }
  
  private def extractFeatures(processed: ProcessedFoodData): FeatureVector = {
    val features = Array(
      processed.energy100g,
      processed.proteins100g,
      processed.carbohydrates100g,
      processed.sugars100g,
      processed.fat100g,
      processed.saturatedFat100g,
      processed.fiber100g,
      processed.salt100g,
      processed.sodium100g
    )
    
    // Normalize features (simple min-max scaling)
    val normalizedFeatures = normalizeFeatures(features)
    
    FeatureVector(processed.id, normalizedFeatures)
  }
  
  private def normalizeFeatures(features: Array[Double]): Array[Double] = {
    // Simple normalization - in production, you'd want to use statistics from training data
    val max = features.max
    val min = features.min
    
    if (max == min) features
    else features.map(f => (f - min) / (max - min))
  }
  
  def processBatch(rawData: List[RawFoodData], batchSize: Int): Try[List[DataProcessingResult]] = {
    Try {
      val batches = rawData.grouped(batchSize).toList
      logger.info(s"Processing ${batches.length} batches of size $batchSize")
      
      batches.zipWithIndex.map { case (batch, index) =>
        logger.info(s"Processing batch ${index + 1}/${batches.length}")
        processRawData(batch).get
      }
    }
  }
}

case class DataProcessingResult(
  processedData: List[ProcessedFoodData],
  featureVectors: List[FeatureVector],
  stats: ProcessingStats
)
