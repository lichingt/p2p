package comp90015.idxsrv.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingDeque;

import comp90015.idxsrv.textgui.ITerminalLogger;

/**
 * A basic IOThread class that accepts connections and puts them
 * onto a blocking queue. If the queue is full then the connection
 * is dropped and a warning is logged.
 * @author aaron
 *
 */
public class IOThread extends Thread {
	private ServerSocket serverSocket;
	private LinkedBlockingDeque<Socket> incomingConnections;
	private ITerminalLogger logger;
	private int timeout;
	
	/**
	 * Create an IOThread, which attempts to the bind to the provided
	 * port with a server socket. The thread must be explicitly started.
	 * @param port the port for the server socket
	 * @param incomingConnections the blocking queue to put incoming connections
	 * @param timeout the timeout value to be set on incoming connections
	 * @param logger an object that implements the terminal logger interface
	 * @throws IOException
	 */
	public IOThread(int port,
			LinkedBlockingDeque<Socket> incomingConnections,
			int timeout,
			ITerminalLogger logger) throws IOException {
		this.timeout = timeout;
		this.logger = logger;
		this.incomingConnections=incomingConnections;
		serverSocket = new ServerSocket(port);
		
	}
	
	/**
	 * Shutdown the server socket, which simply closes it.
	 * @throws IOException
	 */
	public void shutdown() throws IOException {
		serverSocket.close();
	}
	
	@Override
	public void run() {
		logger.logInfo("IO thread running");
		while(!isInterrupted()) {
			try {
				Socket socket = serverSocket.accept();
				try {
					socket.setSoTimeout(this.timeout);
					if(!incomingConnections.offer(socket)) {
						socket.close();
						logger.logWarn("IO thread dropped connection - incoming connection queue is full.");	
					}
				} catch (IOException e) {
					logger.logWarn("Something went wrong with the connection.");
				}
			} catch (IOException e) {
				logger.logError("IO thread failed to accept.");
				break;
			} 
		}
		logger.logInfo("IO thread completed.");
	}
}
