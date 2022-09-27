package comp90015.idxsrv.peer;

import java.io.File;
import java.net.InetAddress;

/**
 * The text gui will expect the file sharing peer to implement the following
 * interface, which represent requested user actions.
 * 
 * @author aaron
 *
 */
public interface IPeer {
	/**
	 * Share the given file with the index server, using the provided secrets.
	 * An error is logged if the provided file is not within the basedir
	 * of the peer, in which case the request is ignored. The shared file
	 * should be added to the gui's shared file table, if the server request
	 * succeeds, using the {@link ISharerGUI} interface.
	 * @param file
	 * @param idxAddress
	 * @param idxPort
	 * @param idxSecret
	 * @param shareSecret
	 */
	public void shareFileWithIdxServer(File file,
			InetAddress idxAddress,
			int idxPort,
			String idxSecret,
			String shareSecret);
	
	/**
	 * Drop (stop) sharing the given file with the index server, using the provided secrets.
	 * @param relativePathname the filename relative to the `basedir`
	 * @param shareRecord describes the shared file to drop
	 * @return true if the sharing table entry should be removed in the gui, false otherwise
	 */
	public boolean dropShareWithIdxServer(String relativePathname,ShareRecord shareRecord);
	
	/**
	 * Search the index server for filenames that contain all the keywords. Obtain
	 * at most `maxhits` results. The results must be added to the gui search table using
	 * the {@link ISharerGUI} interface.
	 * @param keywords
	 * @param maxhits
	 * @param idxAddress
	 * @param idxPort
	 * @param idxSecret
	 */
	public void searchIdxServer(String[] keywords,
			int maxhits,
			InetAddress idxAddress,
			int idxPort,
			String idxSecret);
	
	public void downloadFromPeers(String relativePathname,SearchRecord searchRecord);
	
}
