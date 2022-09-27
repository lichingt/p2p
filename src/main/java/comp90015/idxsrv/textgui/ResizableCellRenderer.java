package comp90015.idxsrv.textgui;

import com.googlecode.lanterna.gui2.TextGUIGraphics;
import com.googlecode.lanterna.gui2.table.Table;

/**
 * A default class for rendering a cell, that supports the resizable table.
 * @author aaron
 *
 * @param <T>
 */
public class ResizableCellRenderer<T> {
	
	public void drawCell(Table<T> table, T cell, int columnIndex, int rowIndex,
			TextGUIGraphics textGUIGraphics) {
		textGUIGraphics.putString(0,0, PeerGUI.fmtWidth(cell.toString(),textGUIGraphics.getSize().getColumns()));
	}

}
