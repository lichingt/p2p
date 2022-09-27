package comp90015.idxsrv.message;

@JsonSerializable
public class WelcomeMsg extends Message {
	
	@JsonElement
	public String msg;
	
	public WelcomeMsg() {
		
	}
	
	public WelcomeMsg(String msg) {
		this.msg = msg;
	}
}
