package addressbook

import org.mongodb.scala.{MongoClient, MongoCollection, Observable}
import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.io.StdIn
import java.io.{File, Serializable}

import org.bson.types.ObjectId
import org.mongodb.scala.model.Filters.{and, equal}

import scala.collection.View.Filter

object Main extends App {

  //CSVUtil.parseCSVToMongo("random_address_data.csv", true)

  Cli.menu()

  //DbUtil.printResult(DbUtil.getContactCollection().deleteOne(and(equal("firstName", "Brandon"), equal("lastName", "Linton"))))

}
