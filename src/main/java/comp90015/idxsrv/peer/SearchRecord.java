package comp90015.idxsrv.peer;

import java.net.InetAddress;

import comp90015.idxsrv.filemgr.FileDescr;

public class SearchRecord {
	
	final public FileDescr fileDescr;
	final public Long numSharers;
	final public InetAddress idxSrvAddress;
	final public Integer idxSrvPort;
	final public String idxSrvSecret;
	final public String sharerSecret;
	
	public SearchRecord(FileDescr fileDescr,
			long numSharers,
			InetAddress idxSrvAddress,
			int idxSrvPort,
			String idxSrvSecret,
			String sharerSecret) {
		this.fileDescr=fileDescr;
		this.numSharers = numSharers;
		this.idxSrvAddress = idxSrvAddress;
		this.idxSrvPort = idxSrvPort;
		this.idxSrvSecret = idxSrvSecret;
		this.sharerSecret = sharerSecret;
	}
}
