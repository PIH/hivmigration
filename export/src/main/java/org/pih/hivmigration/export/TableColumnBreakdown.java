package org.pih.hivmigration.export;

import java.util.List;
import java.util.Map;

/**
 * Encapsulates a database table row
 */
public class TableColumnBreakdown {

	private TableColumn tableColumn;
	private int numNullValues;
	private int numNotNullValues;
	private int numDistinctNonNullValues;
	private Map<Object, Integer> mostFrequentValues;
	private Object minValue;
	private Object maxValue;

	public TableColumnBreakdown() { }

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("==================");
		sb.append("Column: ").append(tableColumn.getTableName()).append(".").append(tableColumn.getColumnName());
		sb.append("DataType: ").append(tableColumn.getDataType()).append("(").append(tableColumn.getLength()).append(")");
		sb.append("Nullable: ").append(tableColumn.getNullable());
		sb.append("==================");

		return sb.toString();
	}

	public TableColumn getTableColumn() {
		return tableColumn;
	}

	public void setTableColumn(TableColumn tableColumn) {
		this.tableColumn = tableColumn;
	}

	public int getNumNullValues() {
		return numNullValues;
	}

	public void setNumNullValues(int numNullValues) {
		this.numNullValues = numNullValues;
	}

	public int getNumNotNullValues() {
		return numNotNullValues;
	}

	public void setNumNotNullValues(int numNotNullValues) {
		this.numNotNullValues = numNotNullValues;
	}

	public int getNumDistinctNonNullValues() {
		return numDistinctNonNullValues;
	}

	public void setNumDistinctNonNullValues(int numDistinctNonNullValues) {
		this.numDistinctNonNullValues = numDistinctNonNullValues;
	}

	public Map<Object, Integer> getMostFrequentValues() {
		return mostFrequentValues;
	}

	public void setMostFrequentValues(Map<Object, Integer> mostFrequentValues) {
		this.mostFrequentValues = mostFrequentValues;
	}

	public Object getMinValue() {
		return minValue;
	}

	public void setMinValue(Object minValue) {
		this.minValue = minValue;
	}

	public Object getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(Object maxValue) {
		this.maxValue = maxValue;
	}
}
