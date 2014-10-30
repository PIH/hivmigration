package org.pih.hivmigration.export;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class ExporterTest {

	@Test
	public void should_getAllTables() throws Exception {

		Exporter exporter = TestUtils.getTestExporter();

		List<String> allTables = exporter.getAllTables();

		System.out.println("All Tables");
		for (String tableName : allTables) {
			System.out.println(tableName);
		}

		BigDecimal numPatients = exporter.uniqueResult("select count(*) from hiv_demographics", BigDecimal.class);
		System.out.println("Found " + numPatients + " patients to export");

		System.out.println("Sites");
		List<Map<String ,Object>> sites = exporter.tableResult("select * from hiv_institutions");
		for (Map<String, Object> row : sites) {
			System.out.println(row);
		}
	}
}
