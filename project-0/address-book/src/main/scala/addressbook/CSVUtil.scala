package addressbook

import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.ObjectId

object CSVUtil {
  def parseCSVToMongo(fileName: String, dropHeaders: Boolean): Unit = {
    println(s"Parsing ${fileName}")

//    val client = MongoClient()
//
//    var contactDAO : ContactDAO = new ContactDAO(client)

    var numAdditions = 0
    var headers = 0
    if (dropHeaders == true) headers += 1 else  0

    val source = io.Source.fromFile(fileName)

    try {
      for(item <- source.getLines().drop(headers)) {
        var content = item.split(",").map(_.trim)
        var newContact: Contact = new Contact(new ObjectId, content(0), content(1), content(2),
                                              content(3), Option(content(4)), Option(content(5)),
                                              Option(content(6)), Option(content(7).toInt))
        DbUtil.insertContact(newContact)
        numAdditions += 1
      }
      println(s"Successfully added ${numAdditions} Contacts")
    } catch {
      case nfe : NumberFormatException => println(s"Failed to parse ${fileName}. Include headers.")
    } finally {
      if (source != null) source.close()
    }

  }
}
