package org.openmrs.hivmigration.export;

import org.junit.Test;

import java.util.List;

public class ExporterTest {

	@Test
	public void should_getAllTables() throws Exception {

		Configuration config = new Configuration();
		config.setDatabaseCredentials(new DatabaseCredentials("jdbc:oracle:thin:@localhost:1521:XE", "hiv", "hiv"));

		Exporter exporter = Exporter.initialize(config);
		List<String> allTables = exporter.getAllTables();

		System.out.println("All Tables");
		for (String tableName : allTables) {
			System.out.println(tableName);
		}
	}
}
