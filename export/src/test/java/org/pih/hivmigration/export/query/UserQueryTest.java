package org.pih.hivmigration.export.query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pih.hivmigration.common.User;
import org.pih.hivmigration.export.DB;
import org.pih.hivmigration.export.TestUtils;

import java.util.Map;

public class UserQueryTest {

	@Before
	public void beforeTest() throws Exception {
		DB.openConnection(TestUtils.getDatabaseCredentials());
	}

	@After
	public void afterTest() throws Exception {
		DB.closeConnection();
	}

	@Test
	public void shouldTestUserQuery() throws Exception {
		Map<Integer, User> users = UserQuery.getUsers();
		TestUtils.assertCollectionSizeMatchesBaseTableSize(users.values(), "users");
		TestUtils.assertAllPropertiesArePopulated(users.values());
	}
}
