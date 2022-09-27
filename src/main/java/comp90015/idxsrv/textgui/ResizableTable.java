package comp90015.idxsrv.textgui;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TerminalTextUtils;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.ThemeDefinition;
import com.googlecode.lanterna.gui2.TextGUIGraphics;
import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.gui2.table.TableCellRenderer;
import com.googlecode.lanterna.gui2.table.TableHeaderRenderer;




/**
 * A table that can resize the contents of its cells to ensure
 * that horizontal scrolling is never activated.
 * @author aaron
 *
 * @param <T>
 */
public class ResizableTable<T> extends Table<T> {
	private final String[] headers;
	private int headerTotalWidth=0;
	private int[] headerWidths;
	private int[] columnMaxWidth;
	private int[] headerPreferredWidths;
	private int[] columnPreferredWidths;
	private ResizableCellRenderer<T> cellRenderer;
	private int expandableColumn;
	private double[] fixedColumns;
	
	/**
	 * Initialize the table with an array of table headers.
	 * @param headers The headers to be used.
	 */
	public ResizableTable(String[] headers) {
		super(headers);
		this.headers = headers;
		headerWidths = new int[headers.length];
		columnMaxWidth = new int[headers.length];
		headerPreferredWidths = new int[headers.length];
		columnPreferredWidths = new int[headers.length];
		expandableColumn=-1;
		fixedColumns = new double[headers.length];
		headerTotalWidth=headers.length;
		cellRenderer=new ResizableCellRenderer<T>();
		for(int i=0;i<headers.length;i++) {
			int width=TerminalTextUtils.getColumnWidth(headers[i]);
			headerTotalWidth+=width;
			headerWidths[i]=width;
			columnMaxWidth[i]=0;
			headerPreferredWidths[i]=width;
			columnPreferredWidths[i]=width;
			fixedColumns[i]=-1;
		}
		
		
		setTableHeaderRenderer(new TableHeaderRenderer<T>() {
			@Override
			public TerminalSize getPreferredSize(Table<T> table, String label, int columnIndex) {
				return new TerminalSize(headerPreferredWidths[columnIndex],1);
			}

			@Override
			public void drawHeader(Table<T> table, String label, int index, TextGUIGraphics textGUIGraphics) {
				ThemeDefinition themeDefinition = table.getThemeDefinition();
		        textGUIGraphics.applyThemeStyle(themeDefinition.getCustom("HEADER", themeDefinition.getNormal()));
				textGUIGraphics.putString(0,0,PeerGUI.fmtWidth(label,textGUIGraphics.getSize().getColumns()));	
			}
	    });
		
		setTableCellRenderer(new TableCellRenderer<T>() {
			@Override
			public TerminalSize getPreferredSize(Table<T> table, T cell, int columnIndex, int rowIndex) {
				return new TerminalSize(columnPreferredWidths[columnIndex],1);
			}

			@Override
			public void drawCell(Table<T> table, T cell, int columnIndex, int rowIndex,
					TextGUIGraphics textGUIGraphics) {
				setTableTheme(table,textGUIGraphics,cell,columnIndex,rowIndex);
				cellRenderer.drawCell(table,cell,columnIndex,rowIndex,textGUIGraphics);
			}
	    });
		
		setEnabled(false);
		
		
	}
	
	/**
	 * There is only one expandable column, and if set it will expand
	 * to take up all remaining space. A value of -1 will specify no
	 * column as expandable.
	 * @param column the column index starting from 0, or -1 if no column
	 */
	public void setExpandableColumn(int column) {
		expandableColumn=column;
	}
	
	/**
	 * A fixed column will attempt to achieve a percentage of the available
	 * space. Multiple columns can be fixed. A percentage value of -1 indicates
	 * the column is not fixed.
	 * @param column the column index starting from 0
	 * @param perc the percentage, or -1
	 */
	public void setFixedColumn(int column, double perc) {
		fixedColumns[column]=perc;
	}
	
	/**
	 * Set a custom cell renderer object if needed.
	 * @param cellRenderer
	 */
	public void setCellRenderer(ResizableCellRenderer<T> cellRenderer) {
		this.cellRenderer = cellRenderer;
	}
	
	private void setTableTheme(Table<T> table, TextGUIGraphics textGUIGraphics,T cell, int columnIndex, int rowIndex) {
		ThemeDefinition themeDefinition = table.getThemeDefinition();
        if((table.getSelectedColumn() == columnIndex && table.getSelectedRow() == rowIndex) ||
                (table.getSelectedRow() == rowIndex && !table.isCellSelection())) {
            if(table.isFocused()) {
                textGUIGraphics.applyThemeStyle(themeDefinition.getActive());
            }
            else {
                textGUIGraphics.applyThemeStyle(themeDefinition.getSelected());
                textGUIGraphics.setForegroundColor(TextColor.ANSI.WHITE_BRIGHT);
            }
            textGUIGraphics.fill(' ');
        } else {
            textGUIGraphics.applyThemeStyle(themeDefinition.getNormal());
            if(cell instanceof TextLabel) {
            	textGUIGraphics.setForegroundColor(((TextLabel)cell).getForegroundColor());
            }
        }
	}
	
	/**
	 * Update the preferred sizes of the table headers. Call this method when
	 * the terminal size has changed.
	 * @param terminalWidth the current width of the terminal
	 */
	public void updateTableHeaderPreferredSizes(int terminalWidth) {
		if(terminalWidth-2<headers.length*2) {
			setVisible(false);
		} else if(terminalWidth-2<headerTotalWidth) {
			int targetSize = terminalWidth-2-headers.length;
			headerPreferredWidths=proportionalReduce(headers,targetSize);
			setVisible(true);
		} else {
			headerPreferredWidths=headerWidths;
			setVisible(true);
		}
		
		updateTableColumnPreferredSizes(terminalWidth);
	}
	
	private static int[] proportionalReduce(String[] headers,int target) {
		int totalSize=0;
		int[] finalSizes=new int[headers.length];
		for(int i=0;i<headers.length;i++) totalSize+=headers[i].length();
		double factor=(double)target/totalSize;
		double residual=0;
		for(int i=0;i<headers.length;i++) {
			double ideal=factor*headers[i].length();
			int actual=(int)Math.max(1, Math.floor(ideal));
			residual+=actual-ideal;
			if(residual<=-1.0) {
				actual+=1;
				residual+=1;
			} else if(residual>=1.0 && actual>1) {
				actual-=1;
				residual+=1;
			}
			finalSizes[i]=actual;
			target-=actual;
		}
		if(target>0) {
			finalSizes[finalSizes.length-1]+=target;
		}
		return finalSizes;
	}
	
	/**
	 * Update the table column preferred sizes. Call this method when the
	 * terminal width has changed.
	 * @param terminalWidth
	 */
	public void updateTableColumnPreferredSizes(int terminalWidth) {
		int[] lossWidths= new int[headers.length];
		int[] effWidths= new int[headers.length];
		int totalWidth=0;
		for(int column=0;column<headers.length;column++) {
			int width = Math.max(columnMaxWidth[column], headerPreferredWidths[column]);
			if(fixedColumns[column]>0) {
				width=Math.max((int)Math.round(terminalWidth*fixedColumns[column]), headerPreferredWidths[column]);
			}
			effWidths[column]=width;
			totalWidth+=width;
			lossWidths[column]=0;
		}
		int renderWidth = totalWidth+headers.length+2;
		if(renderWidth<terminalWidth && expandableColumn>=0) {
			lossWidths[expandableColumn] = -(terminalWidth - renderWidth);
		} else if(renderWidth>terminalWidth) {
			int totalLossWidth=renderWidth - terminalWidth;
			HashMap<Integer,Integer> widths = new HashMap<>();
			for(int i=0;i<headers.length;i++) {
				widths.put(i,Math.max(effWidths[i]-headerPreferredWidths[i],0));
			}
			List<Map.Entry<Integer, Integer> > list =
		               new ArrayList<Map.Entry<Integer, Integer> >(widths.entrySet());
			while(totalLossWidth>0) {
				Collections.sort(list, new Comparator<Map.Entry<Integer, Integer> >() {
		            public int compare(Map.Entry<Integer, Integer> o1,
		                               Map.Entry<Integer, Integer> o2)
		            {
		                return (o2.getValue()).compareTo(o1.getValue());
		            }
		        });
				int max1 = list.get(0).getValue();
				int max2 = list.get(1).getValue();
				int max1index = list.get(0).getKey();
				if(max1>0) {
					
					int delta = max1-max2;
					if(delta==0) {
						delta=1;
					} else if(delta>totalLossWidth) {
						delta=totalLossWidth;
					} 
					
					lossWidths[max1index]+=delta;
					totalLossWidth-=delta;
					widths.put(max1index,max1-delta);
					
				} else {
					break;
				}
			}
		}
		for(int i=0;i<headers.length;i++) {
			columnPreferredWidths[i]=effWidths[i]-lossWidths[i];
		}
		invalidate();
	}
	
	/**
	 * Clear the table contents, which clears the table's model.
	 */
	public void clearTable() {
		getTableModel().clear();
		invalidate();
		setEnabled(false);
		for(int i=0;i<headers.length;i++) {
			columnMaxWidth[i]=0;
			columnPreferredWidths[i]=0;
		}
		invalidate();
	}
	
	/**
	 * Incrementally update the maximum column width for each column.
	 * Call this method after adding a row to the table; the added row must be
	 * the last row in the table. 
	 */
	public void updateColumnMax() {
		int rowIndex=getTableModel().getRowCount()-1;
		for(int i=0;i<headers.length;i++) {
			String entry = getTableModel().getCell(i, rowIndex).toString();
			if(entry!=null) {
				if(TerminalTextUtils.getColumnWidth(entry)>columnMaxWidth[i]) {
					columnMaxWidth[i]=TerminalTextUtils.getColumnWidth(entry);
				}
			}
		}
		invalidate();
	}

	/**
	 * Recompute the maximum width of each column, by examining every cell in
	 * the table.
	 */
	public void recomputeColumnMax() {
		columnMaxWidth = new int[headers.length];
		for(int i=0;i<headers.length;i++) columnMaxWidth[i]=0;
		for(int rowIndex=0;rowIndex<getTableModel().getRowCount();rowIndex++) {
			for(int i=0;i<headers.length;i++) {
				String entry = getTableModel().getCell(i, rowIndex).toString();
				if(entry!=null) {
					if(TerminalTextUtils.getColumnWidth(entry)>columnMaxWidth[i]) {
						columnMaxWidth[i]=TerminalTextUtils.getColumnWidth(entry);
					}
				}
			}
		}
		invalidate();
	}
	
	
}
