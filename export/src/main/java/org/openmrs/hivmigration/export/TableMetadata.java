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
package org.openmrs.hivmigration.export;

import org.openmrs.hivmigration.common.util.ListMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates relevant metadata for a table
 */
public class TableMetadata {

	private String tableName;
	private ListMap<String, String> foreignKeyMap;

	public TableMetadata(String tableName) {
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getForeignKeys(String columnName) {
		List<String> ret = getForeignKeyMap().get(columnName);
		return (ret == null ? new ArrayList<String>() : ret);
	}

	public ListMap<String, String> getForeignKeyMap() {
		if (foreignKeyMap == null) {
			foreignKeyMap = new ListMap<String, String>();
		}
		return foreignKeyMap;
	}

	public void setForeignKeyMap(ListMap<String, String> foreignKeyMap) {
		this.foreignKeyMap = foreignKeyMap;
	}
}
