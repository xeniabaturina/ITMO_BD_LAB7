package com.itmo.datamart.model

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import java.time.LocalDateTime
import com.itmo.datamart.service.DataMartStatus

// Processed food data from MySQL (pre-processed in Lab 5/6)
case class RawFoodData(
  id: Long,
  productName: Option[String],
  brands: Option[String],
  categories: Option[String],
  energy100g: Option[Double],
  proteins100g: Option[Double],
  carbohydrates100g: Option[Double],
  sugars100g: Option[Double],
  fat100g: Option[Double],
  saturatedFat100g: Option[Double],
  fiber100g: Option[Double],
  salt100g: Option[Double],
  sodium100g: Option[Double],
  createdAt: LocalDateTime
)

// Processed food data for ClickHouse
case class ProcessedFoodData(
  id: Long,
  productName: String,
  brands: String,
  categories: String,
  energy100g: Double,
  proteins100g: Double,
  carbohydrates100g: Double,
  sugars100g: Double,
  fat100g: Double,
  saturatedFat100g: Double,
  fiber100g: Double,
  salt100g: Double,
  sodium100g: Double,
  processedAt: LocalDateTime
)

// Feature vector for ML model
case class FeatureVector(
  id: Long,
  features: Array[Double]
) {
  override def toString: String = s"FeatureVector($id, [${features.mkString(", ")}])"
  
  override def equals(obj: Any): Boolean = obj match {
    case other: FeatureVector => 
      id == other.id && features.sameElements(other.features)
    case _ => false
  }
  
  override def hashCode(): Int = {
    val prime = 31
    var result = id.hashCode
    result = prime * result + features.toSeq.hashCode
    result
  }
}

// Model run configuration
case class ModelRunConfig(
  runName: String,
  kClusters: Int,
  maxIter: Int,
  seed: Int
)

// Model run result
case class ModelRunResult(
  runId: Long,
  runName: String,
  silhouetteScore: Double,
  status: String,
  startedAt: LocalDateTime,
  completedAt: Option[LocalDateTime]
)

// Clustering result
case class ClusteringResult(
  id: Long,
  modelRunId: Long,
  processedDataId: Long,
  clusterId: Int,
  distanceToCenter: Double
)

// Cluster center
case class ClusterCenter(
  id: Long,
  modelRunId: Long,
  clusterId: Int,
  energy100g: Double,
  proteins100g: Double,
  carbohydrates100g: Double,
  sugars100g: Double,
  fat100g: Double,
  saturatedFat100g: Double,
  fiber100g: Double,
  salt100g: Double,
  sodium100g: Double
)

// Data processing statistics
case class ProcessingStats(
  totalRecords: Long,
  validRecords: Long,
  invalidRecords: Long,
  outliersRemoved: Long,
  processingTime: Long
)

// API request/response models
case class DataRequest(
  maxRecords: Option[Int] = None,
  filters: Option[Map[String, String]] = None
)

case class DataResponse(
  data: List[FeatureVector],
  stats: ProcessingStats
)

case class ResultRequest(
  modelRunId: Long,
  results: List[ClusteringResult],
  clusterCenters: List[ClusterCenter]
)

case class ResultResponse(
  success: Boolean,
  message: String,
  recordsStored: Int
)

// JSON encoders/decoders for Circe
object FoodData {
  implicit val rawFoodDataEncoder: Encoder[RawFoodData] = deriveEncoder
  implicit val rawFoodDataDecoder: Decoder[RawFoodData] = deriveDecoder
  
  implicit val processedFoodDataEncoder: Encoder[ProcessedFoodData] = deriveEncoder
  implicit val processedFoodDataDecoder: Decoder[ProcessedFoodData] = deriveDecoder
  
  implicit val featureVectorEncoder: Encoder[FeatureVector] = deriveEncoder
  implicit val featureVectorDecoder: Decoder[FeatureVector] = deriveDecoder
  
  implicit val modelRunConfigEncoder: Encoder[ModelRunConfig] = deriveEncoder
  implicit val modelRunConfigDecoder: Decoder[ModelRunConfig] = deriveDecoder
  
  implicit val modelRunResultEncoder: Encoder[ModelRunResult] = deriveEncoder
  implicit val modelRunResultDecoder: Decoder[ModelRunResult] = deriveDecoder
  
  implicit val clusteringResultEncoder: Encoder[ClusteringResult] = deriveEncoder
  implicit val clusteringResultDecoder: Decoder[ClusteringResult] = deriveDecoder
  
  implicit val clusterCenterEncoder: Encoder[ClusterCenter] = deriveEncoder
  implicit val clusterCenterDecoder: Decoder[ClusterCenter] = deriveDecoder
  
  implicit val processingStatsEncoder: Encoder[ProcessingStats] = deriveEncoder
  implicit val processingStatsDecoder: Decoder[ProcessingStats] = deriveDecoder
  
  implicit val dataRequestEncoder: Encoder[DataRequest] = deriveEncoder
  implicit val dataRequestDecoder: Decoder[DataRequest] = deriveDecoder
  
  implicit val dataResponseEncoder: Encoder[DataResponse] = deriveEncoder
  implicit val dataResponseDecoder: Decoder[DataResponse] = deriveDecoder
  
  implicit val resultRequestEncoder: Encoder[ResultRequest] = deriveEncoder
  implicit val resultRequestDecoder: Decoder[ResultRequest] = deriveDecoder
  
  implicit val resultResponseEncoder: Encoder[ResultResponse] = deriveEncoder
  implicit val resultResponseDecoder: Decoder[ResultResponse] = deriveDecoder
  
  implicit val dataMartStatusEncoder: Encoder[DataMartStatus] = deriveEncoder
  implicit val dataMartStatusDecoder: Decoder[DataMartStatus] = deriveDecoder
}
