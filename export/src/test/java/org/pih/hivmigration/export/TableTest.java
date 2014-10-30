package org.pih.hivmigration.export;

import org.junit.Assert;
import org.junit.Test;
import org.pih.hivmigration.common.util.Util;

import java.util.ArrayList;
import java.util.List;

public class TableTest {

	@Test
	public void testAllTablesAreSpecified() throws Exception {

		Exporter exporter = TestUtils.getTestExporter();
		List<String> allTables = exporter.getAllTables();

		String[] tableFiles = {"hivTables", "auditTables", "frameworkTables", "hivViews", "notNeededTables", "peruTables", "warehouseTables"};
		List<String> specifiedTables = new ArrayList<String>();
		for (String tableFile : tableFiles) {
			for (String table : Util.loadFromClasspath(tableFile + ".txt")) {
				Assert.assertTrue("Table " + table + " is specified twice", !specifiedTables.contains(table));
				specifiedTables.add(table);
			}
		}

		List<String> allNotSpecified = new ArrayList<String>(allTables);
		allNotSpecified.removeAll(specifiedTables);
		Assert.assertEquals("Tables " + allNotSpecified + " not specified", 0, allNotSpecified.size());

		List<String> specifiedNotAll = new ArrayList<String>(specifiedTables);
		specifiedNotAll.removeAll(allTables);
		Assert.assertEquals("Tables " + specifiedNotAll + " not in all tables list", 0, specifiedNotAll.size());

		Assert.assertEquals(allTables.size(), specifiedTables.size());
	}
}
