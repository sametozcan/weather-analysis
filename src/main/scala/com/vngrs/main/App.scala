package com.vngrs.main

import java.io.{File, IOException, BufferedWriter, FileWriter}
import com.vngrs.db.{Operations, Connection}
import com.vngrs.io.URLReader
import com.typesafe.config.ConfigFactory
import com.vngrs.spark.AvgCalc
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

object App {

  val properties = ConfigFactory.load("application.properties")
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

    logger.info("Challenge starts!")
    logger.info("Part 1 started...")
    collectData
    logger.info("Part 1 is completed - Data is collected from URLs!")
    logger.info("Part 2 started...")
    mapReduceOperations
    logger.info("Part 2 is completed - Average calculations are done!")
    logger.info("Part 3 started...")
    dbOperations
    logger.info("Part 3 is completed - Data is written to DB!")
    logger.info("End of the challenge!")

  }

  /**
   * Reads data from given URLs and writes to a csv file specified
   */
  def collectData(): Unit ={

    var fileWriter: FileWriter = null
    var bufferedWriter: BufferedWriter = null
    val filePath = properties.getString("file.path")
    // deletes if a file exists before writing to the file
    FileUtils.deleteQuietly(new File(filePath))
    try {
      fileWriter = new FileWriter(new File(filePath))
      bufferedWriter = new BufferedWriter(fileWriter)
      URLReader.writeDataFromUrl(properties.getString("url"), bufferedWriter)
      bufferedWriter.close()
    } catch {
      case e: IOException => {
        logger.error("Error writing to file!")
        e.printStackTrace()
      }
    } finally {
      bufferedWriter.close()
      fileWriter.close()
    }

  }

  /**
   * Calculates monthly average temperatures and saves under folder specified
   */
  def mapReduceOperations(): Unit = {

    AvgCalc.run()

  }

  /**
   * Loads data to table from directory specified
   */
  def dbOperations(): Unit ={

    val conn = Connection.getConnection
    Operations.deleteTable(conn)
    Operations.createTable(conn)
    Operations.loadDataFromDirectory(conn, properties.getString("result.folder.path"))
    Connection.closeConnection(conn)

  }


}
