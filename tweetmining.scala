package com.nmit.spark.tweetmining

import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.rdd._

/**
  *
  *  Problem statement:
  *  We will use the dataset with the 8198 reduced tweets, reduced-tweets.json. 
  *  The data are reduced tweets as the example below:
  *
  *  {"id":"572692378957430785",
  *  "user":"Srkian_nishu :)",
  *  "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
  *  "place":"Orissa",
  *  "country":"India"}
  *
  *  We want to make some computations on the users:
  *  - find all the tweets by user
  *  - find how many tweets each user has
  *  - print the top 10 tweeters
  *
  */

object tweetmining {

  // Create the spark configuration and spark context
  val conf = new SparkConf()
    .setAppName("User mining")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  // Needs an argument which is the path to the tweets file in json format.
  // e.g. "/home/subhrajit/sparkProjects/data/reduced-tweets.json"

  var pathToFile = ""

  def main(args: Array[String]) {
    if (args.length != 1) {
      println()
      println("Dude, I need exactly one argument.")
      println("But you have given me " + args.length +".")
      println("The argument should be path to json file containing a bunch of tweets. esired.")
      System.exit(1)
    }

    pathToFile = args(0)

    // The code below creates an RDD of tweets. Please look at the case class Tweet towards the end
    // of this file.

    val tweets =
      sc.textFile(pathToFile).mapPartitions(TweetUtils.parseFromJson(_))

    // Create an RDD of (user, Tweet).
    // Look at the tweet class. An object of type tweet will have fields "id", "user", "userName" etc.
    // Collect all his tweets by the user. For this, you can use the field "user" of an object
    // in the tweet RDD. 
    // Hint: the Spark API provides a groupBy method
    // The code below should return RDD's with tuples (user, List of user tweets).

    val tweetsByUser = tweets.map(x => (x.user, x)).groupByKey()
    // tweets.take(10).foreach{ case(tweet) => println(tweet.user) }

    // For each user, find the number of tweets he/she has made. 
    // Hint: we need tuples of (user, number of tweets by user)

    val numTweetsByUser = tweetsByUser.map(x => (x._1, x._2.size))

    // Sort the users by the number of their tweets.

    val sortedUsersByNumTweets = numTweetsByUser.sortBy(_._2, ascending=false)

    // Find the Top 10 twitterers and print them to the console.

    sortedUsersByNumTweets.take(10).foreach(println)

  }
}

import com.google.gson._
	
object TweetUtils {
	case class Tweet (
		id : String,
		user : String,
		userName : String,
		text : String,
		place : String,
		country : String,
		lang : String
		)

	
	def parseFromJson(lines:Iterator[String]):Iterator[Tweet] = {
		val gson = new Gson
		lines.map(line => gson.fromJson(line, classOf[Tweet]))
	}
}

/** 
  Whenever you have heavyweight initialization that should be done once for many RDD elements 
  rather than once per RDD element, use mapPartitions() instead of map(). 
  mapPartitions() provides for the initialization to be done once per worker task/thread/partition 
  instead of once per RDD data element for example.

  Example Scenario : if we have 100K elements in a particular RDD partition then we will fire 
  off the function being used by the mapping transformation 100K times when we use map.

  Conversely, if we use mapPartitions then we will only call the particular function one time, 
  but we will pass in all 100K records and get back all responses in one function call.

  There will be performance gain since map works on a particular function so many times, 
  especially if the function is doing something expensive each time that it wouldn't need 
  to do if we passed in all the elements at once(in case of mappartitions).
  * */
