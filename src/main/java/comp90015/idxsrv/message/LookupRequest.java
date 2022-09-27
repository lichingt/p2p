package comp90015.idxsrv.message;

@JsonSerializable
public class LookupRequest extends Message {

	@JsonElement
	public String fileMd5;
	
	@JsonElement
	public String filename;
	
	public LookupRequest() {
		
	}
	
	public LookupRequest(String filename, String fileMd5) {
		this.filename=filename;
		this.fileMd5=fileMd5;
	}
}
