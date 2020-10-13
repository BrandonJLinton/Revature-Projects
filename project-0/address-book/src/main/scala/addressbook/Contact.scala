package addressbook

import org.bson.types.ObjectId

case class Contact(_id: ObjectId, firstName: String, lastName: String, cellNumber: String, email: String,
                   address: Option[String], city: Option[String], state: Option[String], zip: Option[Int])

object Contact {
  def apply(firstName: String, lastName: String, cellNumber: String, email: String,
            address: Option[String], city: Option[String], state: Option[String], zip: Option[Int]) : Contact =
              Contact(new ObjectId(), firstName, lastName, cellNumber, email, address, city, state, zip)
}
