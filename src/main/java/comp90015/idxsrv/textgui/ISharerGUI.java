package comp90015.idxsrv.textgui;

import comp90015.idxsrv.peer.SearchRecord;
import comp90015.idxsrv.peer.ShareRecord;

/**
 * A simple interface for showing tables of sharing and search results to the user.
 * @author aaron
 *
 */
public interface ISharerGUI extends ITerminalLogger {
	/**
	 * Clear the current search results table.
	 */
	public void clearSearchHits();
	
	/**
	 * Clear the current sharing table, i.e. the table of files that are being shared.
	 */
	public void clearShareRecords();
	
	/**
	 * Add an entry to the search hits table, i.e. the table that shows search results
	 * from an index server for a keyword search. 
	 * @param filename the relative pathname of the file being shared
	 * @param searchRecord describes the file that can be downloaded from other peers
	 */
	public void addSearchHit(String filename,SearchRecord searchRecord);
	
	/**
	 * Add an entry to the sharing table, i.e. the table showing the files that the peer is
	 * currently sharing. If the table already contains an entry with the same relativePathname
	 * then the {@link updateShareRecord} method is transparently called.
	 * @param relativePathname the relative pathname to the file
	 * @param shareRecord describes the file that is being shared
	 */
	public void addShareRecord(String relativePathname,ShareRecord shareRecord);
	
	/**
	 * Update an existing share record that has previously been added to the sharing table.
	 * The existing share record is replaced with the given one.
	 * If the table does not contain an entry with the same relativePathname then the
	 * {@link addShareRecord} method is transparently called.
	 * @param relativePathname the relative pathname to the file
	 * @param shareRecord describes the file that is being shared
	 */
	public void updateShareRecord(String relativePathname,ShareRecord shareRecord);
}
