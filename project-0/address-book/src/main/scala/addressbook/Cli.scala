package addressbook

import java.io.FileNotFoundException

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
    println("* Select a Command Below -     *")
    println("* Exit                         *")
    println("********************************")
  }

  def menu() : Unit = {
    var continueMenuLoop = true
    printWelcome()

    while (continueMenuLoop) {
      printOption()

      StdIn.readLine("Enter Command: ") match {
        case commandArgPattern(cmd, arg) if cmd.equalsIgnoreCase("exit") => continueMenuLoop = false
        case notRecognized => println(s"${notRecognized} not a recognized command")
      }
    }
  }

}
