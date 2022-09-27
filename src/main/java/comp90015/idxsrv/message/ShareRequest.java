package comp90015.idxsrv.message;

import comp90015.idxsrv.filemgr.FileDescr;

@JsonSerializable
public class ShareRequest extends Message {

	@JsonElement
	public FileDescr fileDescr;
	
	@JsonElement
	public String filename;
	
	@JsonElement
	public String sharingSecret;
	
	@JsonElement
	public Integer port;
	
	public ShareRequest() {
		
	}
	
	public ShareRequest(FileDescr fileDescr, String filename, String sharingSecret, int port) {
		this.fileDescr=fileDescr;
		this.filename=filename;
		this.sharingSecret=sharingSecret;
		this.port=port;
	}
	
}
