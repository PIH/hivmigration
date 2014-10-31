package org.pih.hivmigration.export;

public class TestUtils {

	public static DatabaseCredentials getDatabaseCredentials() {
		return new DatabaseCredentials("jdbc:oracle:thin:@localhost:1521:XE", "hiv", "hiv");
	}

}
