package com.vngrs.db

import java.io.File
import java.util
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.config.ConfigFactory
import com.vngrs.common.Constants
import org.apache.log4j.Logger
import scala.io.Source

/**
 * Created by sametozcan on 10/11/15.
 */
object Operations {

  val properties = ConfigFactory.load("application.properties")
  private val logger = Logger.getLogger(getClass)

  /**
   * Creates table specified with given parameters
   * @param dynamoDB
   */
  def createTable(dynamoDB: DynamoDB): Unit = {

    val tableName = properties.getString("dynamodb.table.name")
    try {
      val table: Table = dynamoDB.createTable(tableName,
        util.Arrays.asList(
          new KeySchemaElement("year_month", KeyType.HASH)),
        util.Arrays.asList(
          new AttributeDefinition("year_month", ScalarAttributeType.S)),
        new ProvisionedThroughput(properties.getLong("dynamodb.table.read.capacity"), properties.getLong("dynamodb.table.write.capacity")))
      logger.info("Table " + tableName + " is created successfully!")
    }catch{
      case e: Exception => {
        logger.error("Error creating table " + tableName + "!!")
        e.printStackTrace()
      }
    }

  }

  /**
   * Deletes table specified
   * @param dynamoDB
   */
  def deleteTable(dynamoDB: DynamoDB): Unit = {

    val tableName = properties.getString("dynamodb.table.name")

    try{
      val table = dynamoDB.getTable(tableName)
      table.delete()
      logger.info("Table " + tableName + " is deleted successfully!")
    }catch{
      case e: Exception => {
        logger.error("Error deleting table " + tableName + "!!")
        e.printStackTrace()
      }
    }

  }

  /**
   * Reads all the files created by spark under given directory and writes content to the table created
   * @param dynamoDB
   * @param dir Directory where spark writes its results
   */
  def loadDataFromDirectory(dynamoDB: DynamoDB, dir: String): Unit = {

    val table = dynamoDB.getTable(properties.getString("dynamodb.table.name"))

    var date = ""
    var min = 0.0
    var max = 0.0
    var mean = 0.0

    for(file <- new File(dir).listFiles.filter(_.getName.startsWith("part-"))) {

      logger.info("Reading the file located " + file.getAbsolutePath)
      for (line <- Source.fromFile(file).getLines()) {

        val inputArr = line.split(Constants.DELIMITER)
        date = inputArr(0)
        min = inputArr(1).toDouble
        max = inputArr(2).toDouble
        mean = inputArr(3).toDouble

        table.putItem(new Item()
          .withPrimaryKey("year_month", date)
          .withDouble("avg_min", min)
          .withDouble("avg_max", max)
          .withDouble("avg_mean", mean))

      }
      logger.info("Inserted all lines to DB from the file located " + file.getAbsolutePath)
    }

  }

  /**
   * Gets record from table created by date given
   * @param dynamoDB
   * @param date  query parameter
   * @param tableName  table name to send query
   * @return Query results as collection
   */
  def getWeatherByDate(dynamoDB: DynamoDB, date: String, tableName: String) : ItemCollection<QueryOutcome> = {

    val table = dynamoDB.getTable(tableName);

    val querySpec = new QuerySpec()
      .withKeyConditionExpression("year_month = :v1")
      .withValueMap(new ValueMap()
        .withString(":v1", date))

    return table.query(querySpec);

  }


}
