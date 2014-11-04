package org.pih.hivmigration.export;

import java.util.Map;

/**
 * Encapsulates join data that can be passed into a query and used to supplement the table data that is populating an object
 */
public class JoinData {

	private String columnName;
	private String propertyName;
	private Map<Integer, ?> propertyValues;

	public JoinData() { }

	public JoinData(String columnName, String propertyName, Map<Integer, ?> propertyValues) {
		this.columnName = columnName;
		this.propertyName = propertyName;
		this.propertyValues = propertyValues;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}

	public Map<Integer, ?> getPropertyValues() {
		return propertyValues;
	}

	public void setPropertyValues(Map<Integer, ?> propertyValues) {
		this.propertyValues = propertyValues;
	}
}
