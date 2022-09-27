package comp90015.idxsrv.peer;


import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

import comp90015.idxsrv.filemgr.FileDescr;
import comp90015.idxsrv.filemgr.FileMgr;
import comp90015.idxsrv.message.*;
import comp90015.idxsrv.server.IOThread;
import comp90015.idxsrv.server.IndexElement;
import comp90015.idxsrv.textgui.ISharerGUI;



/**
 * Peer class completed for Project 1.
 * @author liching
 * @author nan
 *
 */
public class Peer implements IPeer {

	private IOThread ioThread;
	
	private LinkedBlockingDeque<Socket> incomingConnections;
	
	private ISharerGUI tgui;
	
	private String basedir;
	
	private int timeout;
	
	private int port;

	private Upload upload; // added

	public Peer(int port, String basedir, int socketTimeout, ISharerGUI tgui) throws IOException {
		this.tgui=tgui;
		this.port=port;
		this.timeout=socketTimeout;
		this.basedir=new File(basedir).getCanonicalPath();
		incomingConnections=new LinkedBlockingDeque<Socket>(); // added
		ioThread = new IOThread(port,incomingConnections,socketTimeout,tgui);
		ioThread.start();
		upload = new Upload(incomingConnections, tgui); // added
		upload.start(); // added
	}
	
	public void shutdown() throws InterruptedException, IOException {
		upload.interrupt();
		upload.join();
		ioThread.shutdown();
		ioThread.interrupt();
		ioThread.join();
	}

	/**
	 *
	 * General outline of Peer structure
	 *
	 * Section 1: Implementation of the four functionality as described in the IPeer.java interface definition.
	 * Section 2: Helper methods for writing and reading messages sent and received
	 * Section 3: Helper method for handshake protocol
	 * Class added 1: Download class
	 * Class added 2: Upload class
	 *
	 */



	/**
	 * Section 1: Implementation of the four functionality as described in the IPeer.java interface definition.
	 *
	 * Method 1 of 4: shareFileWithIdxServer
	 * Method 2 of 4: searchIdxServer
	 * Method 3 of 4: dropShareWithIdxServer
	 * Method 4 of 4: downloadFromPeers
	 */
	
	@Override
	public void shareFileWithIdxServer(File file, InetAddress idxAddress, int idxPort, String idxSecret,
			String shareSecret) {

		try {

			// Disallow file selection from outside base directory
			if (!file.getPath().startsWith(this.basedir)) {
				tgui.logWarn("File selected is not within base directory");
				return;
			}

			// Making connection to server
			Socket socket = new Socket(idxAddress, idxPort);
			socket.setSoTimeout(timeout);
			InputStream inputStream = socket.getInputStream();
			OutputStream outputStream = socket.getOutputStream();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

			if (!handshake(idxSecret, bufferedReader, bufferedWriter)){
				return;
			}

			// Get relative path name based on base directory
			String relativePathname = file.getPath().substring(this.basedir.length() + 1);

			// Sends ShareRequest message
			try{
				RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
				FileDescr fileDescr = new FileDescr(randomAccessFile);
				writeMsg(bufferedWriter, new ShareRequest(fileDescr, relativePathname, shareSecret, this.port));
			} catch (FileNotFoundException e) {
				tgui.logWarn("Peer received File Not Found Exception while accessing file to share");
				return;
			}

			// Receives ShareReply message
			Message msg;
			try {
				msg = readMsg(bufferedReader);
			} catch (JsonSerializationException e1) {
				writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
				return;
			}

			// Evaluate whether the message is ShareReply
			ShareReply shareReplyMsg =  new ShareReply();
			if (msg.getClass().getName().equals(ShareReply.class.getName())) {
				shareReplyMsg = (ShareReply) msg;
			}	else {
				writeMsg(bufferedWriter,new ErrorMsg("Expecting ShareReply"));
				return;
			}

			// Obtain information from the Message
			Integer numSharers = shareReplyMsg.numSharers;

			// Build a ShareRecord and add it to the GUI
			FileMgr fileMgr = new FileMgr(file.getPath());
			ShareRecord shareRecord = new ShareRecord(fileMgr, numSharers,
					"seeding", idxAddress, idxPort, idxSecret, shareSecret);
			tgui.addShareRecord(relativePathname, shareRecord);

			fileMgr.closeFile();

			bufferedReader.close();
			bufferedWriter.close();
			socket.close();

			tgui.logInfo("Share file with Server completed");

		} catch (NoSuchAlgorithmException e){
			tgui.logWarn("MD5 hash algorithm is unavailable for file selected to share");
		} catch(IOException e) {
			tgui.logWarn("Peer received io exception while sharing");
		} catch (Exception e){
			tgui.logWarn("Peer received unknown exception while sharing: " + e.getClass() + e.getMessage());
		}
	}

	@Override
	public void searchIdxServer(String[] keywords, 
			int maxhits, 
			InetAddress idxAddress, 
			int idxPort, 
			String idxSecret){

		try {

			// Making connection to server
			Socket socket = new Socket(idxAddress, idxPort);
			socket.setSoTimeout(timeout);
			InputStream inputStream = socket.getInputStream();
			OutputStream outputStream = socket.getOutputStream();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

			if (!handshake(idxSecret, bufferedReader, bufferedWriter)){
				return;
			}

			// Sends SearchRequest message
			writeMsg(bufferedWriter, new SearchRequest(maxhits,keywords));

			// Receives SearchReply message
			Message msg;
			try {
				msg = readMsg(bufferedReader);
			} catch (JsonSerializationException e1) {
				writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
				return;
			}

			// Evaluate whether the message is searchReply
			SearchReply searchReplyMsg = new SearchReply();
			if (msg.getClass().getName().equals(SearchReply.class.getName())) {
				searchReplyMsg = (SearchReply) msg;
			}	else {
				writeMsg(bufferedWriter,new ErrorMsg("Expecting SearchReply"));
				return;
			}

			// Obtain information from the Message
			IndexElement[] hits = searchReplyMsg.hits;
			Integer[] seedCounts = searchReplyMsg.seedCounts;

			tgui.clearSearchHits();
			// Build a searchRecord for each hit and add it to the GUI
			for(int i=0;i<hits.length;i++){
				IndexElement hit = hits[i];
				Integer seedCount = seedCounts[i];
				String filename = hit.filename;
				FileDescr fileDescr = hit.fileDescr;
				long numSharers = seedCount.longValue();
				String sharerSecret = hit.secret;

				SearchRecord searchRecord = new SearchRecord(fileDescr,numSharers,idxAddress,
						idxPort,idxSecret,sharerSecret);
				tgui.addSearchHit(filename,searchRecord);
			}

			tgui.logInfo("Search finished. "  + hits.length + " hits found.");

			bufferedReader.close();
			bufferedWriter.close();
			socket.close();

		} catch(IOException e){
			tgui.logWarn("Peer received io exception while searching");
		} catch (Exception e){
			tgui.logWarn("Peer received unknown exception while searching: " + e.getClass() + e.getMessage());
		}
	}

	@Override
	public boolean dropShareWithIdxServer(String relativePathname, ShareRecord shareRecord) {

		try {

			// Obtain information from ShareRecord
			FileMgr fileMgr = shareRecord.fileMgr;
			// long numSharers = shareRecord.numSharers; // Not required
			// String status = shareRecord.status; // Not required
			InetAddress idxSrvAddress = shareRecord.idxSrvAddress;
			int idxSrvPort = shareRecord.idxSrvPort;
			String idxSrvSecret = shareRecord.idxSrvSecret;
			String sharerSecret = shareRecord.sharerSecret;

			// Making connection to server
			Socket socket = new Socket(idxSrvAddress, idxSrvPort);
			socket.setSoTimeout(timeout);
			InputStream inputStream = socket.getInputStream();
			OutputStream outputStream = socket.getOutputStream();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

			if (!handshake(idxSrvSecret, bufferedReader, bufferedWriter)){
				return false;
			}

			// Sends DropShareRequest message
			String fileMd5 = fileMgr.getFileDescr().getFileMd5();
			writeMsg(bufferedWriter, new DropShareRequest(relativePathname,
					fileMd5, sharerSecret, this.port));

			// Receives DropShareReply message
			Message msg;
			try {
				msg = readMsg(bufferedReader);
			} catch (JsonSerializationException e1) {
				writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
				return false;
			}

			// Evaluate whether the message is DropShareReply
			DropShareReply dropShareReplyMsg = new DropShareReply();
			if (msg.getClass().getName().equals(DropShareReply.class.getName())) {
				dropShareReplyMsg = (DropShareReply) msg;
			}	else {
				writeMsg(bufferedWriter,new ErrorMsg("Expecting DropShareReply"));
				return false;
			}

			// Obtain information from the Message
			Boolean success = dropShareReplyMsg.success;

			// Show result of drop
			if (success){
				tgui.logInfo("The file share has been dropped.");
			}	else {
				tgui.logInfo("The file share drop has failed");
				return false;
			}

			fileMgr.closeFile();

			bufferedReader.close();
			bufferedWriter.close();
			socket.close();

		} catch(IOException e) {
			tgui.logWarn("Peer received io exception while dropping share");
			return false;
		} catch (Exception e){
			tgui.logWarn("Peer received unknown exception dropping share: " + e.getClass() + e.getMessage());
			return false;
		}
		return true;
	}

	@Override
	public void downloadFromPeers(String relativePathname, SearchRecord searchRecord) {
		try{
			// read idxServer information from the searchRecord
			InetAddress idxSrvAddress = searchRecord.idxSrvAddress;
			int idxSrvPort = searchRecord.idxSrvPort;
			String idxSrvSecret = searchRecord.idxSrvSecret;

			// Making connection to server
			Socket socket = new Socket(idxSrvAddress, idxSrvPort);
			socket.setSoTimeout(timeout);
			InputStream inputStream = socket.getInputStream();
			OutputStream outputStream = socket.getOutputStream();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));

			if (!handshake(idxSrvSecret, bufferedReader, bufferedWriter)){
				return;
			}

			// read file related information from the searchRecord
			String fileMD5 = searchRecord.fileDescr.getFileMd5();

			// Send the LookupRequest
			writeMsg(bufferedWriter,new LookupRequest(relativePathname,fileMD5));

			// Receives LookupReply message
			Message msg;
			try {
				msg = readMsg(bufferedReader);
			} catch (JsonSerializationException e1) {
				writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
				return;
			}

			// Evaluate whether the message is LookupReply
			LookupReply lookupReplyMsg = new LookupReply();
			if (msg.getClass().getName().equals(LookupReply.class.getName())) {
				lookupReplyMsg = (LookupReply) msg;
			}	else {
				writeMsg(bufferedWriter,new ErrorMsg("Expecting LookupReply"));
				return;
			}

			// Read information from the LookupReply
			IndexElement[] availableHits = lookupReplyMsg.hits;
			int numAvailablePeers = availableHits.length;

			bufferedReader.close();
			bufferedWriter.close();
			socket.close();

			FileDescr fileDescr;
			int numBlocks;
			// Check whether the availableHits is an empty array
			if(!(numAvailablePeers == 0)){
				fileDescr = availableHits[0].fileDescr;
				numBlocks = availableHits[0].fileDescr.getNumBlocks();
			}else{
				tgui.logInfo("No peer available for " + relativePathname + ". Please try later.");
				return;
			}


			tgui.logInfo(relativePathname + " :Found " + numAvailablePeers + " peers, "
                    + numBlocks + " blocks waiting to be downloaded.");
			tgui.logInfo("Download for " + relativePathname + " starts...");


			// Create a local file and relative fileMgr to store downloaded contents
			FileMgr fileMgr = null;
			try{
				File downloadedFile = new File(this.basedir +'/'+ relativePathname);
				downloadedFile.getParentFile().mkdir();
				downloadedFile.createNewFile();
				fileMgr = new FileMgr(downloadedFile.getPath(),fileDescr);

			} catch(IOException e){
				tgui.logWarn("I/O Exception when creating fileMgr");
				return;
			} catch(NoSuchAlgorithmException e){
				tgui.logError("Download received NoSuchAlgorithmException at fileMgr");
				return;
			}

			// Determine the number of hits to download from -- max 3 hits for each download
			int selectedNum = Math.min(3, Math.min(numBlocks,numAvailablePeers));
			IndexElement[] selectedHits = new IndexElement[selectedNum];

			// Select hits randomly from the available hits
			int[] selectedIndexes = new Random().ints(0, numAvailablePeers).
					distinct().limit(selectedNum).toArray();
			for(int i=0;i<selectedHits.length;i++) selectedHits[i] = availableHits[selectedIndexes[i]];

			// Divide the blocks to download from each selected sharer
			int numBlockPerHit = (int)Math.ceil((double)numBlocks / (double)selectedHits.length);
			for(int i = 0;i < selectedHits.length;i++){
				IndexElement hit = selectedHits[i];

				// Create the blockList for the hit
				int[] blockList = new int[Math.min(numBlockPerHit,numBlocks - numBlockPerHit * i)];
				for(int index=0;index<blockList.length;index++){
					blockList[index] = numBlockPerHit * i + index;
				}

				// Create a download thread for each hit to speed up the download process
				new Download(hit.ip,hit.port,hit.filename,blockList,fileMgr, this.tgui, this.timeout).start();
			}


		} catch (IOException e){
			tgui.logWarn("Peer received io exception while downloading from peers");
		} catch (Exception e){
			tgui.logError("Peer received unknown exception while downloading from peers" + e.getClass() + e.getMessage());
		}
	}



	/**
	 * Section 2: Helper methods for writing and reading messages sent and received
	 */

	private void writeMsg(BufferedWriter bufferedWriter, Message msg) throws IOException {
		tgui.logDebug("sending: "+msg.toString());
		bufferedWriter.write(msg.toString());
		bufferedWriter.newLine();
		bufferedWriter.flush();
	}

	private Message readMsg(BufferedReader bufferedReader) throws IOException, JsonSerializationException {
		String jsonStr = bufferedReader.readLine();
		if (jsonStr != null) {
			Message msg = (Message) MessageFactory.deserialize(jsonStr);
			tgui.logDebug("received: " + msg.toString());
			return msg;
		} else {
			throw new IOException();
		}
	}



	/**
	 * Section 3: Helper method for handshake protocol
	 */
	private boolean handshake(String idxSecret, BufferedReader bufferedReader, BufferedWriter bufferedWriter) throws IOException {

		// 1. Receives welcome message
		Message msg;
		try {
			msg = readMsg(bufferedReader);
		} catch (JsonSerializationException e1) {
			writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
			return false;
		}
		if (msg.getClass().getName().equals(WelcomeMsg.class.getName())){
			WelcomeMsg welcomeMsg = (WelcomeMsg) msg;
			tgui.logInfo(welcomeMsg.msg);
		}	else {
				writeMsg(bufferedWriter,new ErrorMsg("Expecting WelcomeMsg"));
				return false;
		}

		// 2. Sends authenticate request
		writeMsg(bufferedWriter, new AuthenticateRequest(idxSecret));

		// 3. Receives authenticate reply
		try {
			msg = readMsg(bufferedReader);
		} catch (JsonSerializationException e1) {
			writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
			return false;
		}
		if (msg.getClass().getName().equals(AuthenticateReply.class.getName())) {
			AuthenticateReply authenticateReplyMsg = (AuthenticateReply) msg;
			if (authenticateReplyMsg.success) {
				tgui.logInfo("Authentication succeed.");
			} else {
				writeMsg(bufferedWriter, new ErrorMsg("Authentication failed"));
				return false;
			}
		}	else {
				writeMsg(bufferedWriter,new ErrorMsg("Expecting AuthenticateReply"));
			return false;
		}
		return true;
	}



}
