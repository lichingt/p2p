package comp90015.idxsrv.message;

@JsonSerializable
public class DropShareRequest extends Message {
	@JsonElement
	public String filename;
	
	@JsonElement
	public String fileMd5;
	
	@JsonElement
	public String sharingSecret;
	
	@JsonElement
	public Integer port;
	
	public DropShareRequest() {
		
	}
	
	public DropShareRequest(String filename, String fileMd5, String sharingSecret,int port) {
		this.filename=filename;
		this.fileMd5=fileMd5;
		this.sharingSecret=sharingSecret;
		this.port=port;
	}
	
}
