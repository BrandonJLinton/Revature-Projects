package addressbook

import org.bson.codecs.configuration.CodecConfigurationException
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
      println(s"No Results For - ${firstName} ${lastName}")
      println("\n********************************")
    }
  }

  def deleteContactByFullName(firstName: String, lastName : String, contactIndex : Int) : Unit = {
    val resultSet = this.getContactResultSet(firstName, lastName)

    this.printContactsByFullName(firstName, lastName)

    try {
      getResult(this.getContactCollection().deleteOne(equal("_id", resultSet(contactIndex)._id)))
      println(s"Successfully Deleted Result #${contactIndex + 1} : ${firstName} ${lastName}")
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

  def updateLastName(contactId : ObjectId): Unit = {
    val nameRegex = "([A-Za-b][a-zA-Z]*)"

    println("Update Last Name")

    var newLastName : String = null
    do {
      val input = StdIn.readLine("New Last Name: ")
      input match {
        case input if input.matches(nameRegex) => newLastName = input
        case _ => println("Invalid Last Name - Try Again")
      }
    } while (newLastName == null)

    printResult(this.getContactCollection().updateOne(equal("_id", contactId), set("lastName", newLastName)))
  }

  def updatePhoneNumber(contactId : ObjectId): Unit = {
    val phoneNumberRegex = "^[1-9]\\d{2}-\\d{3}-\\d{4}"

    println("Update Phone Number")

    var newPhoneNumber : String = null
    do {
      val input = StdIn.readLine("New Phone Number: ")
      input match {
        case input if (input.matches(phoneNumberRegex)) => newPhoneNumber = input
        case _ => println("Invalid Number - Use Format (XXX-XXX-XXXX)")
      }
    } while (newPhoneNumber == null)

    printResult(this.getContactCollection().updateOne(equal("_id", contactId), set("cellNumber", newPhoneNumber)))
  }

  def updateEmail(contactId : ObjectId): Unit = {
    val emailRegex = "(\\w+)@([\\w\\.]+)"

    println("Update Email")

    var newEmail : String = null
    do {
      val input = StdIn.readLine("New Email: ")
      input match {
        case input if (input.matches(emailRegex)) => newEmail = input
        case _ =>println("Invalid Email - Try Again")
      }
    } while (newEmail == null)

    printResult(this.getContactCollection().updateOne(equal("_id", contactId), set("email", newEmail)))
  }

  def updateAddress(contactId : ObjectId): Unit = {
    val newAddress : String = StdIn.readLine("Address: ") match {
      case "" => ""
      case address : String  => address
    }

    val newCity : String = StdIn.readLine("City: ") match {
      case "" => ""
      case city : String => city
    }

    var newState : String = null
    do {
      val input = StdIn.readLine("State: ")
      input match {
        case input if (input.matches("""[A-Z][A-Z]""")) => newState = input
        case "" => newState = ""
        case _ => newState = null; println("Invalid State - Use Format (XX)")
      }
    } while (newState == null)

    var newZip : Int = 0
    do {
      val input : String = StdIn.readLine("Zip: ")
      input match {
        case input if (input.matches("""[0-9]{5}""")) => newZip = input.toInt
        case "" => newZip = 1
        case _ => newZip = 0; println("Invalid Zip - Must Be 5 Digits")
      }
    } while (newZip == 0)

    getResult(this.getContactCollection().updateOne(
      equal("_id", contactId), set("address", newAddress)
    ))

    getResult(this.getContactCollection().updateOne(
      equal("_id", contactId), set("city", newCity)
    ))

    getResult(this.getContactCollection().updateOne(
      equal("_id", contactId), set("state", newState)
    ))

    if (newZip != 0 && newZip != 1){
      getResult(this.getContactCollection().updateOne(
        equal("_id", contactId), set("zip", newZip)
      ))
    }else if (newZip == 1) {
      getResult(this.getContactCollection().updateOne(
        equal("_id", contactId), set("zip", null)
      ))
    }

  }

  def updateContactByFullName(firstName: String, lastName: String) : Unit = {
    val resultSet = this.getContactResultSet(firstName, lastName)

    if (resultSet.isEmpty) {
      println("No Contacts Found")
    } else {
      this.printContactsByFullName(firstName, lastName)

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
        case _ => println("Invalid Selection")
      }
    }
  }

}
