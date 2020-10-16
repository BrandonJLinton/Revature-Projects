package addressbook

import java.io.FileNotFoundException

import org.bson.types.ObjectId

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
    println("* 4. Search for Contact        *")
    println("* Exit                         *")
    println("********************************")
  }

  def inputSingleContact() : Unit = {
    println("********************************")
    println("Input Contact")

    val cellPhoneRegex = "^[1-9]\\d{2}-\\d{3}-\\d{4}"

    // TODO: Loop on firstName/lastName or it inputs blank

    val firstName : String = StdIn.readLine("First Name: ")

    val lastName : String = StdIn.readLine("Last Name: ")

    var cellPhone : String = null
    var cellLoop = true
    do {
      cellPhone = StdIn.readLine("Cell Number: ")
      if (cellPhone.matches(cellPhoneRegex)) cellLoop = false else println("Invalid Phone Number Format - Try Again")
    } while(cellLoop)

    var email : String = null
    var emailLoop = true
    do {
      email = StdIn.readLine("Email: ")
      if (email.matches("""(\w+)@([\w\.]+)""")) emailLoop = false else println("Invalid Email Format - Try Again")
    } while (emailLoop)

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
      val newState = StdIn.readLine("State: ")
      newState match {
        case newState if (newState.matches("""[A-Z][A-Z]""")) => state = Option(newState)
        case "" => state = None
        case _ => state = null; println("Invalid State - Use XX Format")
      }
    } while (state == null)

    var zip : Option[Int] = null
    do {
      val newZip : String = StdIn.readLine("Zip: ")
      newZip match {
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

    DbUtil.searchForContact(firstName, lastName)
  }

  def deleteContact() : Unit = {
    println("********************************")
    println("Delete Contact")
    val firstName : String = StdIn.readLine("First Name: ")
    val lastName : String = StdIn.readLine("Last Name: ")

    DbUtil.deleteContact(firstName, lastName)
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
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("4") => printContact()
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("exit") => continueMenuLoop = false; println("Shutting Down....")
        case notRecognized => println(s"${notRecognized} not a recognized command")
      }
    }
  }

}
