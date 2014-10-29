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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Encapsulates a database column value
 */
public class ColumnValue {

	//***** PROPERTIES *****

	private String tableName;
	private String columnName;
	private int type;
	private Object value;

	//***** CONSTRUCTORS *****

    public ColumnValue(String tableName, String columnName, int type, Object value) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.type = type;
		this.value = value;
    }

	public String getValueForExport() {
		if (value == null) {
			return "null";
		}
		if (type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGVARCHAR || type == Types.CLOB) {
			return "'" + value.toString().replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
		}
		else if (type == Types.BLOB || type == Types.VARBINARY || type == Types.LONGVARBINARY) {
			try {
				StringBuilder replacement = new StringBuilder();
				replacement.append("0x");
				InputStream in = null;
				if (value instanceof Blob) {
					in = ((Blob)value).getBinaryStream();
				}
				else if (value instanceof byte[]) {
					in = new ByteArrayInputStream((byte[])value);
				}
				else {
					throw new RuntimeException("Don't know handle to handle binary type " + value.getClass().getSimpleName());
				}
				while (true) {
					try {
						int b = in.read();
						if (b < 0) {
							break;
						}
						//MySQL expects two chars for a byte
						replacement.append(String.format("%02x", b));
					} catch (Exception e) {
						throw new SQLException("Error getting column as bytes", e);
					}
				}
				return replacement.toString();
			}
			catch (Exception e) {
				throw new RuntimeException("Error trying to get a binary value in " + tableName + "." + columnName + " for export", e);
			}
		} else if (type == Types.DATE || type == Types.TIMESTAMP) {
			return "'" + value + "'";
		}
		return value.toString();
	}

	@Override
	public String toString() {
		return getValueForExport();
	}

	//***** INSTANCE METHODS *****

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

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
}
