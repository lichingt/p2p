package comp90015.idxsrv.message;

@JsonSerializable
public class ShareReply extends Message {
	
	@JsonElement
	public Integer numSharers;
	
	public ShareReply() {
		
	}
	
	public ShareReply(Integer numSharers) {
		this.numSharers = numSharers;
	}
}
