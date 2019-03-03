package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airportsWithLatitudeOver40").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/airports.text")
    val airports = lines.map(line => line.split(Utils.COMMA_DELIMITER).toList)

    val airportsWithLatitudeOver40 = airports.filter(
      airport => airport(6).toDouble > 40d
    ).map(
        airportOver40 => s"${airportOver40(1)}, ${airportOver40(6)}"
    )

    airportsWithLatitudeOver40.saveAsTextFile("out/airports_by_latitude.text")

  }
}
