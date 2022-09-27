package comp90015.idxsrv.message;

@JsonSerializable
public class DropShareReply extends Message {

	@JsonElement
	public Boolean success;
	
	public DropShareReply() {
		
	}
	
	public DropShareReply(Boolean success) {
		this.success = success;
	}
	
}
