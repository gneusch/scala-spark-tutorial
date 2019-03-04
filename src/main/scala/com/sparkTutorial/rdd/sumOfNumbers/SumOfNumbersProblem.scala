package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("sumOfPrimes").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/prime_nums.text")
    val numbers = lines.flatMap(line => line.split("\t"))
      .map(num => {
        Try(Integer.parseInt(num.trim)).toOption.getOrElse(0)
      })

    val numberList = numbers.collect
    numberList.foreach(number => println(s"$number "))

    val sum = numbers.reduce((x, y) => x + y)
    println(sum)

  }
}
