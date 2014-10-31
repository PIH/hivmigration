package org.pih.hivmigration.export;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pih.hivmigration.common.util.Util;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DBTest {

	@Before
	public void beforeTest() throws Exception {
		DB.openConnection(TestUtils.getDatabaseCredentials());
	}

	@After
	public void afterTest() throws Exception {
		DB.closeConnection();
	}

	@Test
	public void shouldGetAllTables() throws Exception {
		List<String> hivTables = DB.getAllTables();
		int actual = DB.uniqueResult("select count(distinct table_name) from user_tab_columns", BigDecimal.class).intValue();
		Assert.assertEquals(actual, hivTables.size());
	}

	@Test
	public void shouldGetAllColumns() throws Exception {
		List<TableColumn> columns = DB.getAllColumns("hiv_institutions");
		Assert.assertEquals(8, columns.size());
		boolean found = false;
		for (TableColumn column : columns) {
			if (column.getColumnName().equalsIgnoreCase("name")) {
				found = true;
				Assert.assertEquals("HIV_INSTITUTIONS", column.getTableName().toUpperCase());
				Assert.assertEquals("NAME", column.getColumnName().toUpperCase());
				Assert.assertEquals("VARCHAR2", column.getDataType().toUpperCase());
				Assert.assertEquals("128", column.getLength().toUpperCase());
				Assert.assertEquals("Y", column.getNullable().toUpperCase());
			}
		}
		Assert.assertTrue("Name column not found in hiv_institutions", found);
	}

	@Test
	public void shouldGetColumn() throws Exception {
		TableColumn column = DB.getColumn("hiv_institutions", "country");
		Assert.assertEquals("HIV_INSTITUTIONS", column.getTableName().toUpperCase());
		Assert.assertEquals("COUNTRY", column.getColumnName().toUpperCase());
		Assert.assertEquals("CHAR", column.getDataType().toUpperCase());
		Assert.assertEquals("2", column.getLength().toUpperCase());
		Assert.assertEquals("N", column.getNullable().toUpperCase());
	}

	@Test
	public void shouldGetNumberOfNonNullValues() throws Exception {
		int n = DB.getNumberOfNonNullValues("hiv_institutions", "moh_code");
		Assert.assertEquals(17, n);
	}

	@Test
	public void shouldGetNumberOfNullValues() throws Exception {
		int n = DB.getNumberOfNullValues("hiv_institutions", "moh_code");
		Assert.assertEquals(4, n);
	}

	@Test
	public void shouldGetNumberOfDistinctValues() throws Exception {
		int n = DB.getNumberOfDistinctNonNullValues("hiv_institutions", "district_reg");
		Assert.assertEquals(2, n);
	}

	@Test
	public void shouldGetValueBreakdown() throws Exception {
		Map<Object, Integer> values = DB.getValueBreakdown("hiv_institutions", "district_reg");
		Assert.assertEquals(4, values.get("Artibonite").intValue());
		Assert.assertEquals(8, values.get("Centre").intValue());
		Assert.assertEquals(9, values.get(null).intValue());
		Assert.assertEquals(3, values.size());
	}

	@Test
	public void shouldGetValueBreakdownWithLimit() throws Exception {
		Map<Object, Integer> values = DB.getValueBreakdown("hiv_products", "prod_type_flag", 3);
		Assert.assertEquals(278, values.get("MedSupply").intValue());
		Assert.assertEquals(177, values.get("SOP").intValue());
		Assert.assertEquals(173, values.get("OralMed").intValue());
		Assert.assertEquals(3, values.size());
	}

	@Test
	public void shouldGetMinValue() throws Exception {
		Assert.assertEquals(0.075, ((BigDecimal)DB.getMinValue("hiv_products", "strength_dose")).doubleValue());
		Assert.assertEquals("2002-10-08", Util.formatDate((Date) DB.getMinValue("hiv_demographics", "patient_created_date"), "yyyy-MM-dd"));
	}

	@Test
	public void shouldGetMaxValue() throws Exception {
		Assert.assertEquals(82, ((BigDecimal)DB.getMaxValue("hiv_institutions", "institution_id")).intValue());
		Assert.assertEquals("2009-09-01", Util.formatDate((Date) DB.getMaxValue("hiv_product_price_lists", "date_published"), "yyyy-MM-dd"));
	}

	@Test
	public void shouldGetTableColumnBreakdown() throws Exception {
		TableColumnBreakdown productUnits = DB.getColumnBreakdown("hiv_products", "strength_unit");
		Assert.assertEquals(0, productUnits.getNumNullValues());
		Assert.assertEquals(1051, productUnits.getNumNotNullValues());
		Assert.assertEquals(13, productUnits.getNumDistinctNonNullValues());
		Map<Object, Integer> frequentValues = productUnits.getMostFrequentValues();
		Map<String, String> expectedValues = Util.toMap("milligram=435,item=270,ml=167,size=47,unspecified=36,gram=30,gauge=22,litre=20,gallon=7,million_units=6,cm=6,tab=4,microgram=1");
		Assert.assertEquals(expectedValues.size(), frequentValues.size());
		for (String value : expectedValues.keySet()) {
			Assert.assertEquals(Integer.valueOf(expectedValues.get(value)), frequentValues.get(value));
		}
		Assert.assertEquals("cm", productUnits.getMinValue());
		Assert.assertEquals("unspecified", productUnits.getMaxValue());
	}
}
