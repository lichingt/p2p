package comp90015.idxsrv.message;

@JsonSerializable
public class ErrorMsg extends Message {

	@JsonElement
	public String msg;
	
	public ErrorMsg() {
		
	}
	
	public ErrorMsg(String msg) {
		this.msg=msg;
	}
	
}
