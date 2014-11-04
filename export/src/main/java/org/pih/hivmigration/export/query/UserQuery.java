package org.pih.hivmigration.export.query;

import org.pih.hivmigration.common.User;
import org.pih.hivmigration.export.DB;

import java.util.Map;

public class UserQuery {

	private static Map<Integer, User> userCache;

	public static User getUser(Object userId) {
		if (userCache == null) {
			userCache = getUsers();
		}
		return userCache.get(userId.toString());
	}

	/**
	 * USER TABLES
	 * User data in contained within the users, parties, and persons tables.  We are choosing not to include the following:
	 * 	url, screen_name, priv_name, priv_email, email_verified_p, email_bouncing_p, no_alerts_until, last_visit, second_to_last_visit, n_sessions, password_question, password_answer.
	 * 	We considered including and migrating Password question and answer were but ultimately decided against it since they are not
	 * 	encrypted and aren't really needed for migration (users can regenerate their own in the new system)
	 */
	public static Map<Integer, User> getUsers() {
		StringBuilder query = new StringBuilder();
		query.append("select	u.user_id as userId, p.email, n.first_names as firstName, n.last_name as lastName, u.password, u.salt ");
		query.append("from		users u, parties p, persons n ");
		query.append("where		u.user_id = p.party_id ");
		query.append("and		u.user_id = n.person_id ");
		return DB.mapResult(query, User.class);
	}
}
