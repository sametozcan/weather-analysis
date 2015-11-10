package com.vngrs.io

import java.io.BufferedWriter
import java.net.URL

import com.vngrs.common.Constants
import org.apache.log4j.Logger
import org.htmlcleaner.HtmlCleaner

/**
 * Created by sametozcan on 08/11/15.
 */

object URLReader {

  val stringBuilder = new StringBuilder
  private val logger = Logger.getLogger(getClass)

  /**
   * Reads the data on URLS under url parameter, parses it and writes to the file specified
   * @param url Main url
   * @param bufferedWriter
   */
  def writeDataFromUrl(url: String, bufferedWriter: BufferedWriter) = {

    val cleaner = new HtmlCleaner
    val rootNode = cleaner.clean(new URL(url))
    var mean = ""
    var max = ""
    var min = ""
    var date = ""

    val elements = rootNode.getElementsByName("a", true)
    for (elem <- elements) {

      val subUrl = elem.getAttributeByName("href")

      // URL is cleaned since given URL takes Access Forbidden error
      val subRootNode = cleaner.clean(new URL(subUrl.substring(0, 7) + subUrl.substring(11)))

      val dateElement = subRootNode.getElementsByName("h1", true)
      date = dateElement(0).getAllChildren().get(0).toString.substring(6,16)

      val subElements = subRootNode.getElementsByName("div", true)
      mean = subElements(0).getText.toString.trim.split(" ")(2)
      max = subElements(1).getText.toString.trim.split(" ")(2)
      min = subElements(2).getText.toString.trim.split(" ")(2)
      // parameters is cleaned from temperature sign
      mean = mean.substring(0, mean.length-1)
      max = max.substring(0, max.length-1)
      min = min.substring(0, min.length-1)

      bufferedWriter.write(buildRowData(date, min, max, mean))

    }

    logger.info("All URLs are read successfully!")
  }

  /**
   * Concats parameters separating with the delimiter
   * @param date
   * @param min
   * @param max
   * @param mean
   * @return String representing a line of csv file built with parameters
   */
  def buildRowData(date:String, min: String, max: String, mean: String) : String = {

    stringBuilder.setLength(0)

    return stringBuilder.append(date).append(Constants.DELIMITER).append(min).append(Constants.DELIMITER)
      .append(max).append(Constants.DELIMITER).append(mean).append("\n").toString()

  }

}
