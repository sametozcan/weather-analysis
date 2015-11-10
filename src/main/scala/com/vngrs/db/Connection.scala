package com.vngrs.db

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.{Item, DynamoDB, Table}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

/**
 * Created by sametozcan on 09/11/15.
 */
object Connection {

  val properties = ConfigFactory.load("application.properties")
  private val logger = Logger.getLogger(getClass)

  /**
   * Creates connection to DynamoDB with credentials specified
   * @return DynamoDB instance
   */
  def getConnection(): DynamoDB = {

    var dynamoDB : DynamoDB = null
    try {
      val client = new AmazonDynamoDBClient(
        new ProfileCredentialsProvider(properties.getString("dynamodb.credential.file.path"), properties.getString("dynamodb.credential.profile")))
      client.setEndpoint(properties.getString("dynamodb.endpoint"))
      dynamoDB  = new DynamoDB(client)
      logger.info("Successfully connected to DB!")
      return dynamoDB

    }catch{
      case e: Exception => {
        logger.error("Error connecting DynamoDB client!!")
        e.printStackTrace()
        System.exit(1)
      }
    }

    return dynamoDB
  }

  /**
   * Closes given connection
   * @param dynamoDB
   */
  def closeConnection(dynamoDB: DynamoDB): Unit ={

    dynamoDB.shutdown()
    logger.info("Connection is closed successfully!")

  }

}
