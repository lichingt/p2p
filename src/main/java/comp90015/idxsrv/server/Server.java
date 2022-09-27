package comp90015.idxsrv.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;

import java.util.concurrent.LinkedBlockingDeque;

import java.nio.charset.StandardCharsets;

import comp90015.idxsrv.message.AuthenticateReply;
import comp90015.idxsrv.message.AuthenticateRequest;
import comp90015.idxsrv.message.DropShareReply;
import comp90015.idxsrv.message.DropShareRequest;
import comp90015.idxsrv.message.ErrorMsg;
import comp90015.idxsrv.message.JsonSerializationException;
import comp90015.idxsrv.message.LookupReply;
import comp90015.idxsrv.message.LookupRequest;
import comp90015.idxsrv.message.Message;
import comp90015.idxsrv.message.MessageFactory;
import comp90015.idxsrv.message.SearchReply;
import comp90015.idxsrv.message.SearchRequest;
import comp90015.idxsrv.message.ShareReply;
import comp90015.idxsrv.message.ShareRequest;
import comp90015.idxsrv.message.WelcomeMsg;
import comp90015.idxsrv.server.IndexMgr.RETCODE;
import comp90015.idxsrv.textgui.ITerminalLogger;


/**
 * Server protocol implementation for the Index Server, which extends thread
 * and processes an unbounded number of incoming connections until it is interrupted.
 * @author aaron
 *
 */
public class Server extends Thread {
	
	/*
	 * Some private variables.
	 */
	
	private IndexMgr indexMgr;
	
	private LinkedBlockingDeque<Socket> incomingConnections;
	
	private IOThread ioThread;
	
	private String welcome;
	
	private String secret;
	
	private ITerminalLogger logger;
	
	/**
	 * The Server thread must be explicitly started after creating an instance. The
	 * Server starts an independent IOThread to accept connections.
	 * @param port
	 * @param address
	 * @param welcome
	 * @param dir
	 * @param secret
	 * @param socketTimeout
	 * @param logger
	 * @throws IOException
	 */
	public Server(int port,
			InetAddress address,
			String welcome,
			String dir,
			String secret,
			int socketTimeout,
			ITerminalLogger logger) throws IOException {
		this.welcome=welcome;
		this.secret=secret;
		this.logger=logger;
		indexMgr = new IndexMgr();
		incomingConnections=new LinkedBlockingDeque<Socket>();
		ioThread = new IOThread(port,incomingConnections,socketTimeout,logger);
		ioThread.start();
	}
	
	@Override
	public void run() {
		logger.logInfo("Server thread running.");
		while(!isInterrupted()) {
			try {
				Socket socket = incomingConnections.take();
				processRequest(socket);
				socket.close();
			} catch (InterruptedException e) {
				logger.logWarn("Server interrupted.");
				break;
			} catch (IOException e) {
				logger.logWarn("Server received io exception on socket.");
			}
		}
		logger.logInfo("Server thread waiting for IO thread to stop...");
		ioThread.interrupt();
		try {
			ioThread.join();
		} catch (InterruptedException e) {
			logger.logWarn("Interrupted while joining with IO thread.");
		}
		logger.logInfo("Server thread completed.");
	}
	
	
	/**
	 * This method is essentially the "Session Layer" logic, where the session is
	 * short since it consists of exactly one request on the socket, then the socket
	 * is closed.
	 * @param socket
	 * @throws IOException
	 */
	private void processRequest(Socket socket) throws IOException {
		String ip=socket.getInetAddress().getHostAddress();
		int port=socket.getPort();
		logger.logInfo("Server processing request on connection "+ip);
		InputStream inputStream = socket.getInputStream();
		OutputStream outputStream = socket.getOutputStream();
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
		
		/*
		 * Follow the synchronous handshake protocol.
		 */
		
		// write the welcome
		writeMsg(bufferedWriter,new WelcomeMsg(welcome));
		
		// get a message
		Message msg;
		try {
			msg = readMsg(bufferedReader);
		} catch (JsonSerializationException e1) {
			writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
			return;
		}
		
		// check it is an authenticate request
		if(msg.getClass().getName()==AuthenticateRequest.class.getName()) {
			AuthenticateRequest ar = (AuthenticateRequest) msg;
			if(!ar.secret.equals(this.secret)) {
				writeMsg(bufferedWriter,new AuthenticateReply(false));
				return;
			} else {
				writeMsg(bufferedWriter,new AuthenticateReply(true));
			}
		} else {
			writeMsg(bufferedWriter,new ErrorMsg("Expecting AuthenticateRequest"));
			return;
		}
		
		/*
		 * Now get the request and process it. This is a single-request-per-connection
		 * protocol.
		 */
		
		// get the request message
		try {
			msg = readMsg(bufferedReader);
		} catch (JsonSerializationException e) {
			writeMsg(bufferedWriter,new ErrorMsg("Invalid message"));
			return;
		}
		
		// process the request message
		String msgname = msg.getClass().getName();
		if(msgname==ShareRequest.class.getName()) {
			processShareCmd(bufferedWriter,(ShareRequest) msg,ip,port);
		} else if(msgname==DropShareRequest.class.getName()) {
			processDropCmd(bufferedWriter,(DropShareRequest) msg,ip,port);
		} else if(msgname==SearchRequest.class.getName()) {
			processSearchCmd(bufferedWriter,(SearchRequest) msg,ip,port);
		} else if(msgname==LookupRequest.class.getName()) {
			processLookupCmd(bufferedWriter,(LookupRequest) msg,ip,port);
		} else {
			writeMsg(bufferedWriter,new ErrorMsg("Expecting a request message"));
		}
		
		// close the streams
		bufferedReader.close();
		bufferedWriter.close();
	}
	
	/*
	 * Methods to process each of the possible requests.
	 */
	
	private void processShareCmd(BufferedWriter bufferedWriter,ShareRequest msg,String ip, int port) throws IOException {
		if(indexMgr.share(ip, msg.port, msg.fileDescr, msg.filename, msg.sharingSecret)==RETCODE.FAILEDSECRET) {
			writeMsg(bufferedWriter,new ErrorMsg("Failed sharing secret"));
		} else {
			Integer numSharers = indexMgr.lookup(msg.filename,msg.fileDescr.getFileMd5()).size();
			writeMsg(bufferedWriter,new ShareReply(numSharers));
		}			
	}
	
	private void processDropCmd(BufferedWriter bufferedWriter,DropShareRequest msg,String ip, int port) throws IOException {
		RETCODE retcode = indexMgr.drop(ip, msg.port, msg.filename, msg.fileMd5, msg.sharingSecret);
		if(retcode==RETCODE.FAILEDSECRET) {
			writeMsg(bufferedWriter,new ErrorMsg("Failed secret"));
		} else if(retcode==RETCODE.INVALID) {
			writeMsg(bufferedWriter,new ErrorMsg("Not found"));
		} else {
			writeMsg(bufferedWriter,new DropShareReply(true));
		}			
	}
	
	private void processSearchCmd(BufferedWriter bufferedWriter,SearchRequest msg,String ip,int port) throws IOException {
		for(int i=0;i<msg.keywords.length;i++) {
			msg.keywords[i]=msg.keywords[i].toLowerCase();
		}
		ArrayList<IndexElement> hits = indexMgr.search(msg.keywords, msg.maxhits);
		Integer[] seedCounts = new Integer[hits.size()];
		for(int i=0;i<hits.size();i++) {
			seedCounts[i]=indexMgr.lookup(hits.get(i).filename,hits.get(i).fileDescr.getFileMd5()).size();
		}
		writeMsg(bufferedWriter,new SearchReply(hits,seedCounts));
	}
	
	private void processLookupCmd(BufferedWriter bufferedWriter,LookupRequest msg,String ip,int port) throws IOException {
		HashSet<IndexElement> hits = indexMgr.lookup(msg.filename,msg.fileMd5);
		writeMsg(bufferedWriter,new LookupReply(new ArrayList<IndexElement>(hits)));
	}
	
	/*
	 * Methods for writing and reading messages.
	 */
	
	private void writeMsg(BufferedWriter bufferedWriter,Message msg) throws IOException {
		logger.logDebug("sending: "+msg.toString());
		bufferedWriter.write(msg.toString());
		bufferedWriter.newLine();
		bufferedWriter.flush();
	}
	
	private Message readMsg(BufferedReader bufferedReader) throws IOException, JsonSerializationException {
		String jsonStr = bufferedReader.readLine();
		if(jsonStr!=null) {
			Message msg = (Message) MessageFactory.deserialize(jsonStr);
			logger.logDebug("received: "+msg.toString());
			return msg;
		} else {
			throw new IOException();
		}
	}
}
