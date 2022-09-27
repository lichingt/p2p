package comp90015.idxsrv.server;

import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.message.JsonElement;
import comp90015.idxsrv.message.JsonSerializable;

@JsonSerializable
public class IndexElement {
	/**
	 * The IP address (numeric dot format) of the indexed file.
	 */
	@JsonElement
	public String ip;
	
	/**
	 * The port number of the indexed file.
	 */
	@JsonElement
	public Integer port;
	
	/**
	 * The file descriptor in string format.
	 */
	@JsonElement
	public FileDescr fileDescr;
	
	/**
	 * The file name.
	 */
	@JsonElement
	public String filename;
	
	/**
	 * The secret for the index element, required to drop it.
	 */
	@JsonElement
	public String secret;
	
	
	public IndexElement() {
		
	}
	
	public IndexElement(String ip,
			int port,
			FileDescr fileDescr,
			String filename,
			String secret) {
		this.ip=ip;
		this.port=port;
		this.fileDescr=fileDescr;
		this.filename=filename;
		this.secret=secret;
	}
}
