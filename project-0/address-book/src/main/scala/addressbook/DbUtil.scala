package addressbook

import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observable, SingleObservable}
import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.model.Filters.{and, equal}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.io.StdIn
import scala.util.{Failure, Success}

object DbUtil {

  // This is where you would normally add connection string, but not needed for local host
  val client = MongoClient()

  var dbName : String = "addressbookdb"
  var contactCollection : String = "contacts"

  def getContactCollection() : MongoCollection[Contact] = {
    val codecRegistry = fromRegistries(fromProviders(classOf[Contact]), MongoClient.DEFAULT_CODEC_REGISTRY)
    val db = this.client.getDatabase(this.dbName).withCodecRegistry(codecRegistry)
    val collection : MongoCollection[Contact] = db.getCollection(this.contactCollection)
    collection
  }

  def getResult[T](obs: Observable[T]) : Seq[T] = Await.result(obs.toFuture(), Duration(10, SECONDS))

  def printResult[T](obs: Observable[T]) : Unit = getResult(obs).foreach(println(_))

  def insertContact(newContact : Contact) : Unit = {
    this.getContactCollection().insertOne(newContact).toFuture().onComplete {
      case Success(v) => None
      case Failure(e) => println("Failed to add Contact")
    }
  }

  def getContactSeq(firstName : String, lastName: String) : Seq[Contact] =
    this.getResult(this.getContactCollection().find(and(equal("firstName", firstName), equal("lastName", lastName))))

  def searchForContact(firstName : String, lastName : String) : Unit = {
    val resultSet = this.getContactSeq(firstName, lastName)

    if (resultSet.length > 0) {
      var counter = 1
      println("********************************\n")
      resultSet.foreach(c => {
          println(s"Result #${counter} - ")
          println(s"Name:\t\t${c.firstName} ${c.lastName}")
          println(s"Phone:\t\t${c.cellNumber}")
          println(s"Email:\t\t${c.email}")

          var address : String = null
          var city : String = null
          var state : String = null
          var zip : String = null

          c.address match {
            case Some(value) => address = value
            case None => None
          }

          c.city match {
            case Some(value) => city = value
            case None => None
          }

          c.state match {
            case Some(value) => state = value
            case None => None
          }

          c.zip match {
            case Some(value) => zip = value.toString
            case None => None
          }

          println(s"Address:\t${if (address != null) address else ""}\n\t\t\t" +
            s"${if (city != null) city else ""} ${if (state != null) state else ""} "+
            s"${if (zip != null) zip else ""}\n")

          counter += 1
        })
      println("********************************")
    } else {
      println("********************************\n")
      println(s"${firstName} ${lastName} Not Found")
      println("\n********************************")
    }
  }

  def deleteContact(firstName: String, lastName : String) : Unit = {
    val resultSet = this.getContactSeq(firstName, lastName)

    this.searchForContact(firstName, lastName)

    val selectedContact : Int = StdIn.readLine("Enter Result # to Delete Contact: ").toInt - 1

    printResult(DbUtil.getContactCollection().deleteOne(equal("_id", resultSet(selectedContact)._id)))

  }

}
