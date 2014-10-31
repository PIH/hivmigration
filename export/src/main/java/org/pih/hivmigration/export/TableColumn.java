package org.pih.hivmigration.export;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a database table row
 */
public class TableColumn {

	private String tableName;
	private String columnName;
	private String dataType;
	private String length;
	private String nullable;

	public TableColumn() { }

	public boolean isDataType(String toCheck) {
		return dataType.equalsIgnoreCase(toCheck);
	}

	public boolean isText() {
		return isDataType("VARCHAR2") || isDataType("CHAR") || isDataType("CLOB");
	}

	public boolean isNumber() {
		return isDataType("NUMBER");
	}

	public boolean isDate() {
		return isDataType("DATE");
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getLength() {
		return length;
	}

	public void setLength(String length) {
		this.length = length;
	}

	public String getNullable() {
		return nullable;
	}

	public void setNullable(String nullable) {
		this.nullable = nullable;
	}
}
