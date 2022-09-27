package comp90015.idxsrv.textgui;

/**
 * A class that can log messages to the terminal.
 * @author aaron
 *
 */
public interface ITerminalLogger {
	/**
	 * Log a message that informational, the program is operating as expected.
	 * @param msg
	 */
	public void logInfo(String msg);
	
	/**
	 * Log a message that is a warning, the user should be extra aware of something.
	 * @param msg
	 */
	public void logWarn(String msg);
	
	/**
	 * Log a message that is an error, the program is not operating as expected.
	 * @param msg
	 */
	public void logError(String msg);
	
	/**
	 * Log a message that is for debugging purposes, only the programmers are interested in this.
	 * @param msg
	 */
	public void logDebug(String msg);
}
