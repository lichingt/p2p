package comp90015.idxsrv.textgui;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.regex.Pattern;

import com.googlecode.lanterna.gui2.BasicWindow;
import com.googlecode.lanterna.gui2.Button;
import com.googlecode.lanterna.gui2.GridLayout;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.TextBox;
import com.googlecode.lanterna.gui2.Window;

/**
 * A basic configuration window for the users to change
 * the default parameters, such as server ip address, etc.
 * @author aaron
 *
 */
public class ConfigWindow extends BasicWindow {

	private InetAddress idxSrvAddress;
	private int idxSrvPort;
	private String idxSrvSecret="server123";
	private String shareSecret="sharer123";
	private int maxhits=1000;
	
	private TextBox configIndexServer;
	private TextBox configPortNumber;
	private TextBox configServerSecret;
	private TextBox configSharerSecret;
	private TextBox configMaxhits;
	
	@SuppressWarnings("unused")
	private PeerGUI peerGUI;
	
	/**
	 * Create the configuration window with default values. The default
	 * value for maxhits is always 1000.
	 * @param idxSrvAddress default index server address
	 * @param idxSrvPort default index server port
	 * @param idxSrvSecret default index server secret
	 * @param shareSecret default sharing secret
	 * @param peerGUI the {@link PeerGUI object that this window is managed by}
	 */
	public ConfigWindow(InetAddress idxSrvAddress,
			int idxSrvPort,
			String idxSrvSecret,
			String shareSecret,
			PeerGUI peerGUI) {
		this.idxSrvAddress=idxSrvAddress;
		this.idxSrvPort=idxSrvPort;
		this.idxSrvSecret=idxSrvSecret;
		this.shareSecret=shareSecret;
		this.peerGUI = peerGUI;
		
		Panel configPanel = new Panel();
	    configPanel.setLayoutManager(new GridLayout(2));

        configPanel.addComponent(new Label("Index Server"));
        configIndexServer = new TextBox(idxSrvAddress.getHostName()).addTo(configPanel);

        configPanel.addComponent(new Label("Port Number"));
        configPortNumber = new TextBox(""+idxSrvPort).setValidationPattern(Pattern.compile("[0-9]*")).addTo(configPanel);

        configPanel.addComponent(new Label("Server Secret"));
        configServerSecret = new TextBox(idxSrvSecret).addTo(configPanel);
        
        configPanel.addComponent(new Label("Sharer Secret"));
        configSharerSecret = new TextBox(shareSecret).addTo(configPanel);
        
        configPanel.addComponent(new Label("Max hits"));
        configMaxhits = new TextBox(""+maxhits).setValidationPattern(Pattern.compile("[0-9]*")).addTo(configPanel);
        
        final ConfigWindow cw = this;
        new Button("Ok", new Runnable() {
            @Override
            public void run() {
            	try {
					InetAddress idxServer = InetAddress.getByName(configIndexServer.getText());
					cw.idxSrvAddress=idxServer;
					peerGUI.logInfo("Using server host: "+idxServer.getHostName());
				} catch (UnknownHostException e) {
					peerGUI.logError("Could not get the address of the index server.");
				}
            	try {
            		int port = Integer.parseInt(configPortNumber.getText());
            		cw.idxSrvPort = port;
            		peerGUI.logInfo("Using server port: "+port);
            	} catch (NumberFormatException e) {
            		peerGUI.logError("Could not parse the port number.");
            	}
            	cw.idxSrvSecret = configServerSecret.getText();
            	peerGUI.logInfo("Using server secret: "+cw.idxSrvSecret);
            	cw.shareSecret = configSharerSecret.getText();
            	peerGUI.logInfo("Using sharer secret: "+cw.shareSecret);
            	try {
            		int m = Integer.parseInt(configMaxhits.getText());
            		maxhits = m;
            		peerGUI.logInfo("Using maxhits: "+maxhits);
            	} catch (NumberFormatException e) {
            		peerGUI.logError("Could not parse max hits.");
            	}
                peerGUI.setMainWindowActive();
            }
        }).addTo(configPanel);
        
        new Button("Cancel", new Runnable() {
            @Override
            public void run() {
                peerGUI.setMainWindowActive();
            }
        }).addTo(configPanel);

        // Create window to hold the panel
        setComponent(configPanel);
        setHints(Arrays.asList(Window.Hint.CENTERED));
	}
	
	/**
	 * Set the window's fields with the configuration values.
	 * Call this method before making the configuration window active
	 * if you want the configuration window to reflect the current
	 * configuration values, else the window's fields will contain the
	 * values that it previously held.
	 */
	public void prepareToBeActive() {
		configIndexServer.setText(idxSrvAddress.getHostName());
		configPortNumber.setText(""+idxSrvPort);
		configServerSecret.setText(idxSrvSecret);
		configSharerSecret.setText(shareSecret);
		configMaxhits.setText(""+maxhits);
	}
	
	/*
	 * Getters and settings for the configuration values.
	 */
	
	public InetAddress getIdxSrvAddress() {
		return idxSrvAddress;
	}

	public void setIdxSrvAddress(InetAddress idxSrvAddress) {
		this.idxSrvAddress = idxSrvAddress;
	}

	public int getIdxSrvPort() {
		return idxSrvPort;
	}

	public void setIdxSrvPort(int idxSrvPort) {
		this.idxSrvPort = idxSrvPort;
	}

	public String getIdxSrvSecret() {
		return idxSrvSecret;
	}

	public void setIdxSrvSecret(String idxSrvSecret) {
		this.idxSrvSecret = idxSrvSecret;
	}

	public String getShareSecret() {
		return shareSecret;
	}

	public void setShareSecret(String shareSecret) {
		this.shareSecret = shareSecret;
	}

	public int getMaxhits() {
		return maxhits;
	}

	public void setMaxhits(int maxhits) {
		this.maxhits = maxhits;
	}
}
