package addressbook

import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observable, SingleObservable}
import org.mongodb.scala.bson.codecs.Macros._
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters.{and, equal}
import org.mongodb.scala.model.Updates.set

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

  def getContactResultSet(firstName : String, lastName: String) : Seq[Contact] =
    this.getResult(this.getContactCollection().find(and(equal("firstName", firstName), equal("lastName", lastName))))

  def insertContact(newContact : Contact) : Unit = {
    this.getContactCollection().insertOne(newContact).toFuture().onComplete {
      case Success(v) => None
      case Failure(e) => println("Failed to add Contact")
    }
  }

  def printContactsByFullName(firstName : String, lastName : String) : Unit = {
    val resultSet = this.getContactResultSet(firstName, lastName)

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

  def deleteContactByFullName(firstName: String, lastName : String, contactIndex : Int) : Unit = {
    val resultSet = this.getContactResultSet(firstName, lastName)

    this.printContactsByFullName(firstName, lastName)

    try {
      getResult(this.getContactCollection().deleteOne(equal("_id", resultSet(contactIndex)._id)))
      println(s"Successfully deleted Result #${contactIndex + 1} : ${firstName} ${lastName}")
    } catch {
      case oob : IndexOutOfBoundsException => println("Invalid Contact")
    }
  }

  def updateFirstName(contactId : ObjectId): Unit = {
    val nameRegex = "([A-Za-b][a-zA-Z]*)"

    println("Update First Name")

    var newFirstName : String = null
    do {
      val input = StdIn.readLine("New First Name: ")
      input match {
        case input if input.matches(nameRegex) => newFirstName = input
        case _ => println("Invalid First Name - Try Again")
      }
    } while (newFirstName == null)

    printResult(this.getContactCollection().updateOne(equal("_id", contactId), set("firstName", newFirstName)))
  }

  def updateLastName(contactId : ObjectId): Unit = ???

  def updatePhoneNumber(contactId : ObjectId): Unit = ???

  def updateEmail(contactId : ObjectId): Unit = ???

  def updateAddress(contactId : ObjectId): Unit = ???

  //def getResultSetByName(firstName : String, lastName : String) : ???

  def updateContactByFullName(firstName: String, lastName: String) : Unit = {
    val resultSet = this.getContactResultSet(firstName, lastName)

    this.printContactsByFullName(firstName, lastName)


    //contactIndex
    val selectedContact : Int = StdIn.readLine("Enter Result # to Edit Contact: ").toInt - 1

    val contactId : ObjectId = resultSet(selectedContact)._id

    println("********************************")
    println("* 1. Update First Name         *")
    println("* 2. Update Last Name          *")
    println("* 3. Update Phone Number       *")
    println("* 4. Update Email              *")
    println("* 5. Update Address            *")
    println("********************************")

    val selectedOption : Int = StdIn.readLine("Select Option: ").toInt
    println("********************************")

    selectedOption match {
      case selectedOption if (selectedOption == 1) => updateFirstName(contactId)
      case selectedOption if (selectedOption == 2) => updateLastName(contactId)
      case selectedOption if (selectedOption == 3) => updatePhoneNumber(contactId)
      case selectedOption if (selectedOption == 4) => updateEmail(contactId)
      case selectedOption if (selectedOption == 5) => updateAddress(contactId)
    }

  }

}
