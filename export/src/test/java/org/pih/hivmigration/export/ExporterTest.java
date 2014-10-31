package org.pih.hivmigration.export;

import org.junit.Test;
import org.pih.hivmigration.common.User;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;

public class ExporterTest {

	@Test
	public void shouldGetAllTables() throws Exception {

		Exporter exporter = TestUtils.getTestExporter();
		try {
			List<String> hivTables = ExportUtil.getTablesSpecifiedInResource("hivTables");
			for (String table : hivTables) {
				System.out.println(table);
				System.out.println("=====================");
				List<TableColumn> columns = exporter.getAllColumns(table);
				for (TableColumn column : columns) {
					BigDecimal numVals = exporter.getNumberOfNonNullValues(table, column.getColumnName());
					System.out.println(column.getColumnName() + " => " + numVals);
				}
				System.out.println(" ");
			}
		}
		finally {
			exporter.destroy();
		}
	}

	@Test
	public void shouldGetAllUsers() throws Exception {
		Exporter exporter = TestUtils.getTestExporter();
		try {
			List<User> users = exporter.getUsers();
			for (User user : users) {
				System.out.println(ExportUtil.toJson(user));
			}
		}
		finally {
			exporter.destroy();
		}
	}
}
