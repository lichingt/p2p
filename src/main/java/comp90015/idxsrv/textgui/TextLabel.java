package comp90015.idxsrv.textgui;

import com.googlecode.lanterna.gui2.Label;

/**
 * An extension of {@link Label} that overrides the {@link toString} method
 * to call {@link getText} rather than return an object reference string.
 * @author aaron
 *
 */
public class TextLabel extends Label {

	public TextLabel(String text) {
		super(text);
	}

	public String toString() {
		return getText();
	}
}
