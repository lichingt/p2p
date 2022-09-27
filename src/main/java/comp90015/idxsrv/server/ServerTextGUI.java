package comp90015.idxsrv.server;

import comp90015.idxsrv.textgui.ITerminalLogger;

/**
 * I did not have time to write a terminal GUI for the server :-[
 * @author aaron
 *
 */
public class ServerTextGUI implements ITerminalLogger {

	@Override
	public void logInfo(String msg) {
		System.out.println("Info: "+msg);
	}

	@Override
	public void logWarn(String msg) {
		System.out.println("Warn: "+msg);
	}

	@Override
	public void logError(String msg) {
		System.out.println("Error: "+msg);
	}

	@Override
	public void logDebug(String msg) {
		System.out.println("Debug: "+msg);
	}
}
