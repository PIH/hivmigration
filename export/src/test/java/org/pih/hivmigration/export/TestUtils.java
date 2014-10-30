package org.pih.hivmigration.export;

public class TestUtils {

	public static Exporter getTestExporter() {
		Configuration config = new Configuration();
		config.setDatabaseCredentials(new DatabaseCredentials("jdbc:oracle:thin:@localhost:1521:XE", "hiv", "hiv"));
		return Exporter.initialize(config);
	}

}
