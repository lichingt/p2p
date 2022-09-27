package comp90015.idxsrv;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.cli.*;

import comp90015.idxsrv.server.Server;
import comp90015.idxsrv.server.ServerTextGUI;

/**
 * The main class for the Index Server.
 * @author aaron
 *
 */
public class IdxSrv 
{	
	private static Options options = new Options();
	
	private static void help() {
		final PrintWriter writer = new PrintWriter(System.out);
		HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp("[-h] [-p <PORT>] [-a <HOSTIP>] [-w <WELCOME>] [-s <SECRET>] [-t <TIMEOUTMS>]", options);
	    writer.flush();
	    System.exit(0);
	}
	
    public static void main( String[] args ) throws IOException
    {
    	int port = 3200;
    	String welcome = "Welcome to the default IdxSrv implementation for COMP90015 SM2 2022.";
    	String dir = System.getProperty("user.dir");
    	String secret = "server123";
    	int timeout = 1000;
    	InetAddress address = InetAddress.getByName("localhost");
    	Option helpOption = new Option("h","help",false,"help");
    	helpOption.setRequired(false);
    	options.addOption(helpOption);
    	Option portOption = new Option("p","port",true,"port number; default ["+port+"]");
    	portOption.setRequired(false);
    	options.addOption(portOption);
    	Option addressOption = new Option("a","address",true,"advertised network address; default ["+address.getHostName()+"]");
    	addressOption.setRequired(false);
    	options.addOption(addressOption);
    	Option welcomeOption = new Option("w","welcome",true,"the welcome message; defauft ["+welcome+"]");
    	welcomeOption.setRequired(false);
    	options.addOption(welcomeOption);
    	Option secretOption = new Option("s","secret",true,"the secret required to access this server; default ["+secret+"]");
    	secretOption.setRequired(false);
    	options.addOption(secretOption);
    	Option timeoutOption = new Option("t","timeout",true,"the default socket timeout in milliseconds; default ["+timeout+"]");
    	timeoutOption.setRequired(false);
    	options.addOption(timeoutOption);
    	CommandLineParser parser = new DefaultParser();
    	CommandLine cmd;
    	ServerTextGUI stg = new ServerTextGUI();
		try {
			cmd = parser.parse( options, args);
			if(cmd.hasOption("h")) {
	    		help();
	    	}
	    	if(cmd.hasOption("p")) {
	    		try {
	    			port = Integer.parseInt(cmd.getOptionValue("p"));
	    			if(port<0) {
	    				port=0;
	    			}
	    		} catch (NumberFormatException e) {
	    			stg.logWarn("The port number should be an integer greater than zero ["+cmd.getOptionValue("p")+"]");
	    		}
	    	}
	    	if(cmd.hasOption("a")) {
	    		try {
	    			address = InetAddress.getByName(cmd.getOptionValue("a"));
	    		} catch (UnknownHostException e) {
	    			stg.logWarn("The supplied address could not be obtained ["+cmd.getOptionValue("a")+"]");
	    		}
	    	}
	    	if(cmd.hasOption("w")) {
	    		welcome=cmd.getOptionValue("w");
	    	}
	    	if(cmd.hasOption("s")) {
	    		secret=cmd.getOptionValue("s");
	    	}
	    	if(cmd.hasOption("t")) {
	    		try {
	    			timeout = Integer.parseInt(cmd.getOptionValue("t"));
	    			if(timeout<0) {
	    				timeout=0;
	    			}
	    		} catch (NumberFormatException e) {
	    			stg.logWarn("The socket timeout (in milliseconds) should be an integer greater than zero ["+cmd.getOptionValue("p")+"]");
	    		}
	    	}
		} catch (ParseException e1) {
			help();
		}
		
		stg.logInfo(welcome);
    	stg.logInfo("using internet address ["+address.getHostName()+":"+port+"]");
    	stg.logInfo("server secret ["+secret+"]");
    	stg.logInfo("socket timeout ["+timeout+"]");
    	
    	Server server = new Server(port,address,welcome,dir,secret,timeout,stg);
    	server.start();
    	try {
			server.join();
		} catch (InterruptedException e) {
			stg.logError("Could not join with the server.");
		}
    }
}
