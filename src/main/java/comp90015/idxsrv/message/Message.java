package comp90015.idxsrv.message;

/**
 * Overrides the toString method to provide a string
 * in JSON format that represents the message.
 * @author aaron
 *
 */
public class Message {

	@Override
	public String toString()  {
		try {
			return MessageFactory.serialize(this);
		} catch (JsonSerializationException e) {
			e.printStackTrace();
			return null;
		}
	}
}
