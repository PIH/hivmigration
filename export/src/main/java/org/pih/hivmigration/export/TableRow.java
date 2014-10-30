/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.pih.hivmigration.export;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates a database table row
 */
public class TableRow {

	private String tableName;
	private Map<String, ColumnValue> columnValueMap;

	public TableRow(String tableName) {
		this.tableName = tableName;
	}

	public Object getRawValue(String columnName) {
		return getColumnValueMap().get(columnName).getValue();
	}

	public void setRawValue(String columnName, Object value) {
		getColumnValueMap().get(columnName).setValue(value);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumns() {
		return new ArrayList<String>(getColumnValueMap().keySet());
	}

	public Map<String, ColumnValue> getColumnValueMap() {
		if (columnValueMap == null) {
			columnValueMap = new LinkedHashMap<String, ColumnValue>();
		}
		return columnValueMap;
	}

	public void addColumnValue(String columnName, ColumnValue columnValue) {
		getColumnValueMap().put(columnName, columnValue);
	}

	public void addColumnValue(String columnName, int type, Object value) {
		getColumnValueMap().put(columnName, new ColumnValue(tableName, columnName, type, value));
	}
}
