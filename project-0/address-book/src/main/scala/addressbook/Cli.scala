package addressbook

import java.io.FileNotFoundException

import addressbook.DbUtil.printResult
import org.bson.types.ObjectId
import org.mongodb.scala.model.Filters.equal

import scala.io.StdIn
import scala.util.matching.Regex

object Cli {

  val commandArgPattern : Regex = "(\\w+)\\s*(.*)".r

  def printWelcome() : Unit = {
    println("********************************")
    println("* Welcome to Your Address Book *")
    println("********************************")
  }

  def printOption() : Unit = {
    println("********************************")
    println("* 1. Import CSV                *")
    println("* 2. Input Contact             *")
    println("* 3. Delete Contact            *")
    println("* 4. Edit Contact              *")
    println("* 5. Search for Contact        *")
    println("* Exit                         *")
    println("********************************")
  }

  def inputSingleContact() : Unit = {
    println("********************************")
    println("Input Contact")

    val cellPhoneRegex = "^[1-9]\\d{2}-\\d{3}-\\d{4}"
    val nameRegex = "([A-Za-b][a-zA-Z]*)"
    val emailRegex = "(\\w+)@([\\w\\.]+)"

    var firstName : String = null
    do {
      var input = StdIn.readLine("First Name: ")
      input match {
        case input if input.matches(nameRegex) => firstName = input
        case _ => println("Invalid First Name - Try Again")
      }
    } while (firstName == null)

    var lastName : String = null
    do {
      val input = StdIn.readLine("Last Name: ")
      input match {
        case input if input.matches(nameRegex) => lastName = input
        case _ => println("Invalid Last Name - Try Again")
      }
    } while (lastName == null)

    var cellPhone : String = null
    do {
      val input = StdIn.readLine("Phone Number: ")
      input match {
        case input if (input.matches(cellPhoneRegex)) => cellPhone = input
        case _ => println("Invalid Number - Use Format (XXX-XXX-XXXX)")
      }
    } while(cellPhone == null)

    var email : String = null
    do {
      val input = StdIn.readLine("Email: ")
      input match {
        case input if (input.matches(emailRegex)) => email = input
        case _ =>println("Invalid Email - Try Again")
      }
    } while (email == null)

    val address : Option[String] = StdIn.readLine("Address: ") match {
      case "" => None
      case address : String  => Option(address)
    }

    val city : Option[String] = StdIn.readLine("City: ") match {
      case "" => None
      case city : String => Option(city)
    }

    var state : Option[String] = null
    do {
      val input = StdIn.readLine("State: ")
      input match {
        case newState if (newState.matches("""[A-Z][A-Z]""")) => state = Option(newState)
        case "" => state = None
        case _ => state = null; println("Invalid State - Use Format (XX)")
      }
    } while (state == null)

    var zip : Option[Int] = null
    do {
      val input : String = StdIn.readLine("Zip: ")
      input match {
        case newZip if (newZip.matches("""[0-9]{5}""")) => zip = Option(newZip.toInt)
        case "" => zip = None
        case _ => zip = null; println("Invalid Zip - Must Be 5 Digits")
      }
    } while (zip == null)

    val newContact = new Contact(new ObjectId, firstName, lastName, cellPhone, email, address, city, state, zip)
    DbUtil.insertContact(newContact)

    println("********************************")
    println("Successful Added Contact")
  }

  def importCSV() : Unit = {
    println("********************************")
    println("Import CSV")
    val fileName : String = StdIn.readLine("Input File Name: ")
    val hasHeaders : Boolean = StdIn.readLine("Include Headers? (yes/no) ") match {
      case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("no") => false
      case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("yes") => true
    }
    try {
      CSVUtil.parseCSVToMongo(fileName, hasHeaders)
    } catch {
      case fnf : FileNotFoundException => println(s"Failed to open ${fileName}")
    }

  }

  def printContact() : Unit = {
    println("********************************")
    println("Search for Contact")
    val firstName : String = StdIn.readLine("First Name: ")
    val lastName : String = StdIn.readLine("Last Name: ")

    DbUtil.printContactsByFullName(firstName, lastName)
  }

  def deleteContact() : Unit = {
    println("********************************")
    println("Delete Contact")
    val firstName : String = StdIn.readLine("First Name: ")
    val lastName : String = StdIn.readLine("Last Name: ")

    DbUtil.printContactsByFullName(firstName, lastName)
    var selectedContact : Int = 0
    try{
      selectedContact = StdIn.readLine("Enter Result # to Delete Contact: ").toInt - 1
      DbUtil.deleteContactByFullName(firstName, lastName, selectedContact)
    } catch {
      case numFormat : NumberFormatException => println("Invalid Contact")
    }

  }

  def editContact(): Unit = {
    println("********************************")
    println("Edit Contact")
    val firstName : String = StdIn.readLine("First Name: ")
    val lastName : String = StdIn.readLine("Last Name: ")

    try {
      DbUtil.updateContactByFullName(firstName, lastName)
    } catch {
      case numFormat : NumberFormatException => println("Invalid Selection")
      case oob : IndexOutOfBoundsException => println("Invalid Contact")
    }

  }

  def menu() : Unit = {
    var continueMenuLoop = true
    printWelcome()

    while (continueMenuLoop) {
      printOption()

      StdIn.readLine("Enter Command: ") match {
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("1") => importCSV()
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("2") => inputSingleContact()
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("3") => deleteContact()
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("4") => editContact()
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("5") => printContact()
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("exit") => continueMenuLoop = false; println("Shutting Down....")
        case notRecognized => println(s"${notRecognized} is an invalid command")
      }
    }
  }

}
