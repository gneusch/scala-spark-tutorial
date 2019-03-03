package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airportsInUsa").setMaster("local[3]") //local mode 3 worker thread in the local box
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/airports.text")
    //val airports = lines.map(line => line.split(",").toList)
    val airports = lines.map(line => line.split(Utils.COMMA_DELIMITER).toList)

    //println(airports.first())

    val usaAirports = airports.filter(airport => airport(3).contains("United States"))
    val airportCityNamePairs = usaAirports map {
      usaAirport => s"${usaAirport(1)}, ${usaAirport(2)}"
    }

    airportCityNamePairs.saveAsTextFile("out/airports_in_usa.txt")

  }
}
