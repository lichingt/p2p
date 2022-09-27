package comp90015.idxsrv;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import comp90015.idxsrv.peer.Peer;
import comp90015.idxsrv.textgui.PeerGUI;

/**
 * Main class for the file sharing peer. Handles options and starts up
 * the terminal text gui for the peer. The gui allows the user to control
 * the peer, including some configuration settings, and provides a general
 * interface to quit the file sharing peer, that terminates the program.
 * @author aaron
 *
 */
public class Filesharer {
	private static Options options = new Options();
	
	private static void help() {
		final PrintWriter writer = new PrintWriter(System.out);
		HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp("[-h] [-p <LOCALPORT>] [-sp <IDXSRVPORT>] [-a <LOCALHOSTIP>] [-sa <IDXSRVHOSTIP>] [-d <SHAREDIR>] [-w <WELCOME>] [-s <SHARERSECRET>] [-ss <SERVERSECRET>] [-t <TIMEOUTMS>]", options);
	    writer.flush();
	    System.exit(0);
	}
	public static void main( String[] args ) throws IOException, InterruptedException{
		/*
		 * Set some default values.
		 */
		
		// a welcome message to use as part of the peer-to-peer handshake
    	String welcome = "Welcome to the default Filesharer implementation for COMP90015 SM2 2022.";
    	
    	// the canonical pathname to the current working directory, which defaults to the base
    	// directory of the files that can be shared
    	String dir = new File(System.getProperty("user.dir")).getCanonicalPath();
    	
    	/*
    	 * So called "in the clear" password style security - not a very good way to
    	 * secure things these days...
    	 */
    	
    	String secret = "sharer123"; // the secret used by this peer to share files
    	String serverSecret = "server123"; // the secret used by the index server, to gain access
    	
    	/*
    	 * Using the InetAddress but really just for the hostname to IP address conversion, not for the port
    	 * numbers.
    	 */
    	InetAddress address = InetAddress.getByName("localhost"); // default hostname of this peer
    	InetAddress idxSrvAddress = InetAddress.getByName("localhost"); // default hostname of the index server
    	int idxSrvPort = 3200; // the port of the index server
		int port = 3201; // the port this peer uses for other peers to connect to
		int timeout = 1000; // the default socket timeout in milliseconds for idle sockets
    	
    	/*
    	 * Specify command line options to override the defaults.
    	 */
    	Option helpOption = new Option("h","help",false,"help");
    	helpOption.setRequired(false);
    	options.addOption(helpOption);
    	Option portOption = new Option("p","port",true,"sharing port number; default ["+port+"]");
    	portOption.setRequired(false);
    	options.addOption(portOption);
    	Option idxSrvPortOption = new Option("sp","sport",true,"index server port number; default ["+idxSrvPort+"]");
    	idxSrvPortOption.setRequired(false);
    	options.addOption(idxSrvPortOption);
    	Option addressOption = new Option("a","address",true,"advertised sharing network address; default ["+address.getHostName()+"]");
    	addressOption.setRequired(false);
    	options.addOption(addressOption);
    	Option idxSrvAddressOption = new Option("sa","saddress",true,"index server network address; default ["+idxSrvAddress.getHostName()+"]");
    	idxSrvAddressOption.setRequired(false);
    	options.addOption(idxSrvAddressOption);
    	Option dirOption =  new Option("d","dir",true,"base directory for sharing; default ["+dir+"]");
    	dirOption.setRequired(false);
    	options.addOption(dirOption);
    	Option welcomeOption = new Option("w","welcome",true,"the welcome message; defauft ["+welcome+"]");
    	welcomeOption.setRequired(false);
    	options.addOption(welcomeOption);
    	Option secretOption = new Option("s","secret",true,"the sharer secret used to share files; default ["+secret+"]");
    	secretOption.setRequired(false);
    	options.addOption(secretOption);
    	Option serverSecretOption = new Option("ss","ssecret",true,"the index server secret; default ["+serverSecret+"]");
    	serverSecretOption.setRequired(false);
    	options.addOption(serverSecretOption);
    	Option timeoutOption = new Option("t","timeout",true,"the default socket timeout in milliseconds; default ["+timeout+"]");
    	timeoutOption.setRequired(false);
    	options.addOption(timeoutOption);
    	
    	/*
    	 * Parse the command line options. This will override the default values when
    	 * it can, will try to ignore parsing exceptions, and check some values to see
    	 * if they are usable, etc... referred to generally as "sanity checking".
    	 */
    	CommandLineParser parser = new DefaultParser();
    	CommandLine cmd;
		try {
			cmd = parser.parse( options, args);
			if(cmd.hasOption("h")) {
	    		help();
	    	}
	    	if(cmd.hasOption("p")) {
	    		try {
	    			port = Integer.parseInt(cmd.getOptionValue("p"));
	    		} catch (NumberFormatException e) {
	    			System.out.println("Warning: Could not parse supplied port number ["+cmd.getOptionValue("p")+"]");
	    		}
	    	}
	    	if(cmd.hasOption("sp")) {
	    		try {
	    			idxSrvPort = Integer.parseInt(cmd.getOptionValue("sp"));
	    		} catch (NumberFormatException e) {
	    			System.out.println("Warning: Could not parse supplied index server port number ["+cmd.getOptionValue("sp")+"]");
	    		}
	    	}
	    	if(cmd.hasOption("a")) {
	    		try {
	    			address = InetAddress.getByName(cmd.getOptionValue("a"));
	    		} catch (UnknownHostException e) {
	    			System.out.println("Warning: The supplied address could not be obtained ["+cmd.getOptionValue("a")+"]");
	    		}
	    	}
	    	if(cmd.hasOption("sa")) {
	    		try {
	    			idxSrvAddress = InetAddress.getByName(cmd.getOptionValue("sa"));
	    		} catch (UnknownHostException e) {
	    			System.out.println("Warning: The supplied index server address could not be obtained ["+cmd.getOptionValue("sa")+"]");
	    		}
	    	}
	    	if(cmd.hasOption("w")) {
	    		welcome=cmd.getOptionValue("w");
	    	}
	    	if(cmd.hasOption("d")) {
	    		String udir=cmd.getOptionValue("d");
	    		Path path = Paths.get(udir);
	    		if(!Files.exists(path)) {
	    			System.out.println("Warning: The supplied directory does not exist ["+udir+"]");
	    		} else {
	    			dir=new File(udir).getCanonicalPath();
	    		}
	    	}
	    	if(cmd.hasOption("s")) {
	    		secret=cmd.getOptionValue("s");
	    	}
	    	if(cmd.hasOption("ss")) {
	    		serverSecret=cmd.getOptionValue("ss");
	    	}
	    	if(cmd.hasOption("t")) {
	    		try {
	    			timeout = Integer.parseInt(cmd.getOptionValue("t"));
	    			if(timeout<0) {
	    				timeout=0;
	    			}
	    		} catch (NumberFormatException e) {
	    			System.out.println("Warning: The socket timeout (in milliseconds) should be an integer greater than zero ["+cmd.getOptionValue("p")+"]");
	    		}
	    	}
		} catch (ParseException e1) {
			help();
		}
		
		/*
		 * Create a terminal text gui for the peer.
		 */
    	PeerGUI textGUI = new PeerGUI(idxSrvAddress,
    			idxSrvPort,
    			serverSecret,
    			secret);
    	
    	/*
    	 * Log some information about the configuration settings.
    	 */
    	textGUI.logInfo(welcome);
    	textGUI.logInfo("using internet address ["+address.getHostName()+":"+port+"]");
    	textGUI.logInfo("using basedir ["+dir+"] - all filenames will be relative to this basedir");
    	textGUI.logInfo("using sharer secret ["+secret+"]");
    	textGUI.logInfo("socket timeout ["+timeout+"]");
    	
    	/*
    	 * Start up a peer.
    	 */
		Peer peer = new Peer(port,dir,timeout,textGUI);
		
		/*
		 * Tell the gui about the peer.
		 */
		textGUI.setPeer(peer);
		
		/*
		 * Give control over to the gui.
		 */
		textGUI.start(); // a blocking call which gives control to the GUI
		
		/*
		 * Tell the peer to shutdown since the user quit the gui.
		 */
		peer.shutdown();
		
    }
}
