package comp90015.idxsrv.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import comp90015.idxsrv.filemgr.FileDescr;

/**
 * A simple class for managing the files that are currently being shared, and tracking
 * the sharers.
 * @author aaron
 *
 */
public class IndexMgr {
	
	public enum RETCODE {
		SUCCESS,
		INVALID,
		FAILEDSECRET
	}
	
	/**
	 * A map from a file's MD5 hash to the set of elements that share that file.
	 */
	private HashMap<String,HashSet<IndexElement>> md5Map;
	
	/**
	 * A map from a sharer key IP:PORT:FILENAME:FILEMD5 to the index element for that unique
	 * sharing of the file.
	 */
	private HashMap<String,IndexElement> sharerMap;
	
	/**
	 * Creates a new index manager.
	 */
	public IndexMgr() {
		md5Map=new HashMap<>();
		sharerMap=new HashMap<>();
	}
	
	/**
	 * Creates an index element that shares the file. If the file is already being
	 * shared by a sharer at an identical ip address and port number, then the secret
	 * must match the existing element's secret for this call to succeed.
	 * @param ip the ip address of the sharer
	 * @param port the port number of the sharer
	 * @param fileDescrStr the string format of the file descriptor being shared
	 * @param filename the filename of the file being shared
	 * @param secret the secret required to drop this index element
	 * @return {@link RETCODE.SUCCESS} if added or {@link RETCODE.FAILEDSECRET} if the element already exists and the provided secret does not match
	 */
	public RETCODE share(String ip, 
			int port, 
			FileDescr fileDescr, 
			String filename,
			String secret) {
		// Create the new index element to share.
		IndexElement element = new IndexElement(ip,port,fileDescr,filename,secret);
		String fileMd5 = fileDescr.getFileMd5(); 
		String sharerKey = ip+":"+port+":"+filename+":"+fileMd5;
		
		// Drop an identical index element for this sharer if it already exists.
		// The secret must match for this to be successful.
		if(sharerMap.containsKey(sharerKey)) {
			if(drop(ip,port,filename,fileMd5,secret)==RETCODE.FAILEDSECRET) {
				return RETCODE.FAILEDSECRET;
			}
		}
		
		// Add the element to be shared
		sharerMap.put(sharerKey, element);
		if(!md5Map.containsKey(fileDescr.getFileMd5())){
			md5Map.put(fileDescr.getFileMd5(),new HashSet<IndexElement>());
		}
		HashSet<IndexElement> md5set = md5Map.get(fileDescr.getFileMd5());
		md5set.add(element);
		return RETCODE.SUCCESS;
	}
	
	/**
	 * Drop an element from the index.
	 * @param ip the ip address of the sharer
	 * @param port the port number of the sharer
	 * @param filename the filename to drop
	 * @param fileMd5 the MD5 hash of the file being shared
	 * @param secret the secret required to drop this index element
	 * @return {@link RETCODE.SUCCESS} if dropped, {@link RETCODE.INVALID} if not found, {@link RETCODE.FAILEDSECRET} if the provided secret does not match
	 */
	public RETCODE drop(String ip,
			int port,
			String filename,
			String fileMd5,
			String secret) {
		String sharerKey = ip+":"+port+":"+filename+":"+fileMd5;
		if(sharerMap.containsKey(sharerKey)) {
			IndexElement existingElement = sharerMap.get(sharerKey);
			if(existingElement.secret.equals(secret)) {
				HashSet<IndexElement> md5set = md5Map.get(fileMd5);
				md5set.remove(existingElement);
				if(md5set.isEmpty()) {
					md5Map.remove(fileMd5);
				}
				sharerMap.remove(sharerKey);
				return RETCODE.SUCCESS;
			}
			return RETCODE.FAILEDSECRET;
		}
		return RETCODE.INVALID;
	}
	
	/**
	 * Do a basic keyword search for elements where all keywords are
	 * contained in the element's filename. Return up to maxhits responses,
	 * in arbitrary order. Keywords should be provided
	 * in lower case.
	 * @param keywords the array of keywords to check for, lower cased
	 * @param maxhits the maximum number of hits to return
	 * @return an array of elements, possibly empty
	 */
	public ArrayList<IndexElement> search(String[] keywords, int maxhits) {
		int hits=0;
		HashSet<String> md5s = new HashSet<String>();
		ArrayList<IndexElement> hitElements = new ArrayList<IndexElement>();
		for(IndexElement element : sharerMap.values()) {
			if(md5s.contains(element.fileDescr.getFileMd5())) {
				continue;
			}
			boolean hit=true;
			for(int k=0; k<keywords.length;k++) {
				if(!element.filename.toLowerCase().contains(keywords[k])) {
					hit=false;
					break;
				}
			}
			if(hit) {
				md5s.add(element.fileDescr.getFileMd5());
				hitElements.add(element);
				hits++;
				if(hits==maxhits) break;
			}
		}
		return hitElements;
	}

	/**
	 * Return the set of all elements corresponding to a given file MD5 hash.
	 * @param fileMd5 the MD5 hash to lookup
	 * @return a set of elements, possibly empty
	 */
	public HashSet<IndexElement> lookup(String filename,String fileMd5){
		if(md5Map.containsKey(fileMd5)){
			HashSet<IndexElement> hits = new HashSet<>();
			for(IndexElement ie : md5Map.get(fileMd5)) {
				if(ie.filename.equals(filename)) {
					hits.add(ie);
				}
			}
			return hits;
		} else {
			return new HashSet<IndexElement>();
		}
	}
	
}
