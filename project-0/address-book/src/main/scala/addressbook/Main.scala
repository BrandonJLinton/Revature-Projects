package addressbook

import org.mongodb.scala.{MongoClient, MongoCollection, Observable}
import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.io.StdIn
import java.io.{File, Serializable}

import org.bson.types.ObjectId

object Main extends App {

//  val codecRegistry = fromRegistries(fromProviders(classOf[Contact]), MongoClient.DEFAULT_CODEC_REGISTRY)
//  // This is where you would normally add connection string, but not needed for local host
//  val client = MongoClient()
//  val db = client.getDatabase("addressbookdb").withCodecRegistry(codecRegistry)
//  val collection : MongoCollection[Contact] = db.getCollection("contacts")
//
//  def getResult[T](obs: Observable[T]) : Seq[T] = {
//    Await.result(obs.toFuture(), Duration(10, SECONDS))
//  }
//
//  def printResult[T](obs: Observable[T]): Unit = {
//    getResult(obs).foreach(println(_))
//  }
//
//  printResult(collection.find())
//
//  printResult(collection.insertOne(Contact("Brandon", "Linton", "", "linton97v@gmail.com", None, None, None, None)))
//
//  printResult(collection.find())

//  var newContact = Contact("Brandon", "Linton", "123412341234", "linton97v@gmail.com", None, None, None, None)
//
//  DbUtil.insertContact(newContact)


//  println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
//  var firstName = StdIn.readLine("First Name")
//  var lastName = StdIn.readLine("Last Name")
//
//  println(firstName + lastName)
  //Contact(firstName, lastName, cellPhone, email, address, city, state, zip)

//  val Contact(_id, firstName, lastName, cellPhone, email, address, city, state, zip) = Contact(new ObjectId, "Brandon", "Linton", "", "linton97v@gmail.com", None, None, None, None)
//
//  println(Contact.unapply(Contact(new ObjectId, "New", "Linton", "", "linton97v@gmail.com", None, None, None, None)))

  //implicit val contactDecoder: RowDecoder[Contact] = RowDecoder.decoder(1, 0, 2, 4)(Contact.apply)

  //CSVUtil.parseCSVToMongo("random_address_data.csv", true)

  Cli.menu()

}
