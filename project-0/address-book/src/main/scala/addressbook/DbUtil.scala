package addressbook

import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observable, SingleObservable}
import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
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


  // TODO: Make this function return a bool to check for completeness
  //  :: Check by adding .toFuture().onComplete and case Success or Failure
  def insertContact(newContact : Contact) : Unit = {
    this.getContactCollection().insertOne(newContact).toFuture().onComplete {
      case Success(v) => None
      case Failure(e) => println("Failed to add Contact")
    }
  }

}
