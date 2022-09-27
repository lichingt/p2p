package comp90015.idxsrv.peer;

import java.net.InetAddress;

import comp90015.idxsrv.filemgr.FileMgr;

public class ShareRecord {

	final public FileMgr fileMgr;
	final public Long numSharers;
	final public String status;
	final public InetAddress idxSrvAddress;
	final public Integer idxSrvPort;
	final public String idxSrvSecret;
	final public String sharerSecret;
	
	public ShareRecord(FileMgr fileMgr,
			long numSharers,
			String status,
			InetAddress idxSrvAddress,
			int idxSrvPort,
			String idxSrvSecret,
			String sharerSecret) {
		this.fileMgr = fileMgr;
		this.numSharers = numSharers;
		this.status = status;
		this.idxSrvAddress = idxSrvAddress;
		this.idxSrvPort = idxSrvPort;
		this.idxSrvSecret = idxSrvSecret;
		this.sharerSecret = sharerSecret;
	}
}
