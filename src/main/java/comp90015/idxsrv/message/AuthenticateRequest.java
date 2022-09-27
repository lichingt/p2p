package comp90015.idxsrv.message;

@JsonSerializable
public class AuthenticateRequest extends Message {
	
	@JsonElement
	public String secret;
	
	public AuthenticateRequest() {
		
	}
	
	public AuthenticateRequest(String secret) {
		this.secret=secret;
	}

}
