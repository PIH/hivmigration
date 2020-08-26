package org.pih.hivmigration.etl.sql

class UserMigrator extends SqlMigrator {

    void migrate() {

        executeMysql('''
            create table hivmigration_users (
                source_user_id int,
                user_uuid char(38),
                person_uuid char(38),
                person_id int,
                user_id int,
                username varchar(50),
                email varchar(100),
                first_name varchar(100),
                last_name varchar(100),
                password varchar(255),
                salt varchar(255),
                member_state varchar(100),
                KEY `source_user_id_idx` (`source_user_id`),
                UNIQUE KEY `user_uuid_idx` (`user_uuid`)
            );
        ''')
        loadFromOracleToMySql(
                '''
                    insert into hivmigration_users(source_user_id, email, first_name, last_name, password, salt, member_state)
                    values (?, ?, ?, ?, ?, ?, ?)
                ''',
                '''
                    select u.USER_ID as SOURCE_USER_ID,
                           p.EMAIL,
                           n.FIRST_NAMES as FIRST_NAME,
                           n.LAST_NAME,
                           u.PASSWORD,
                           u.SALT,
                           mr.MEMBER_STATE
                    from users u,
                         parties p,
                         persons n,
                         acs_rels ar,
                         membership_rels mr
                    where u.USER_ID = p.PARTY_ID
                      and u.USER_ID = n.PERSON_ID
                      and u.USER_ID = ar.OBJECT_ID_TWO
                      and ar.REL_ID = mr.REL_ID
                      and ar.REL_TYPE = 'membership_rel\'
                      and mr.MEMBER_STATE IS NOT NULL
                    order by u.user_id
                '''
        )
        executeMysql('''
            update hivmigration_users set user_uuid = uuid();
            update hivmigration_users set person_uuid = uuid();
            update hivmigration_users set username = left(email, locate('@', email) - 1);
            update hivmigration_users set username = concat(username, '-', source_user_id) where username in (select username from users);
        ''')
        executeMysql('''
              INSERT INTO person (creator, date_created, uuid)
                SELECT 1, now(), person_uuid from hivmigration_users;
                
              UPDATE        hivmigration_users hu
              INNER JOIN    person p on p.uuid = hu.person_uuid
              SET           hu.person_id = p.person_id;
              
              INSERT INTO person_name (person_id, given_name, family_name, preferred, creator, date_created, uuid) 
                SELECT  person_id, first_name, last_name, 1, 1, now(), uuid()
                FROM    hivmigration_users;
             
              INSERT INTO users (person_id, username, system_id, creator, date_created, uuid, password, salt)
                SELECT  person_id, username, source_user_id, 1, now(), user_uuid, password, salt
                FROM    hivmigration_users;
                
              UPDATE        hivmigration_users hu
              INNER JOIN    users u on u.uuid = hu.user_uuid
              SET           hu.user_id = u.user_id;

              INSERT INTO user_property (user_id, property, property_value) 
                SELECT  user_id, 'notificationAddress', email
                FROM    hivmigration_users;

              UPDATE        users u
              INNER JOIN    hivmigration_users hu on hu.user_id = u.user_id
              SET           retired = TRUE, retired_by = 1, date_retired = now(), retire_reason = hu.member_state
              WHERE         hu.member_state in ('deleted','banned');
        ''')

        // for some reason, after the insert into person_name, the auto increment ends up at 1027; nothing seems wrong with the insert
        setAutoIncrement("person_name", "(select (max(person_name_id)+1) from person_name)")
    }

    void revert() {
        if (tableExists("hivmigration_users")) {
            executeMysql('''
                delete from user_property where user_id in (select u.user_id from users u, hivmigration_users hu where u.uuid = hu.user_uuid);
                delete from user_role where user_id in (select u.user_id from users u, hivmigration_users hu where u.uuid = hu.user_uuid);
                delete from users where uuid in (select user_uuid from hivmigration_users);
                
                delete from name_phonetics;
                delete from person_name where person_id in (select p.person_id from person p, hivmigration_users hu where p.uuid = hu.person_uuid);
                delete from person where uuid in (select person_uuid from hivmigration_users);
                
                drop table hivmigration_users;
            ''')
        }

        // reset the table auto_increments after removing users
        setAutoIncrement("person_name", "(select (max(person_name_id)+1) from person_name)")
        setAutoIncrement("person", "(select (max(person_id)+1) from person)")
        setAutoIncrement("users", "(select (max(user_id)+1) from users)")
    }
}
