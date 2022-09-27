package comp90015.idxsrv.message;

@JsonSerializable
public class AuthenticateReply extends Message {

	@JsonElement
	public Boolean success;
	
	public AuthenticateReply() {
		
	}
	
	public AuthenticateReply(boolean success) {
		this.success=success;
	}
}
