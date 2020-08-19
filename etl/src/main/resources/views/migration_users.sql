/*
User data in contained within the users, parties, and persons tables.  We are choosing not to include the following:
url, screen_name, priv_name, priv_email, email_verified_p, email_bouncing_p, no_alerts_until, last_visit,
second_to_last_visit, n_sessions, password_question, password_answer.
We considered including and migrating Password question and answer were but ultimately decided against it since
they are not encrypted and aren't really needed for migration (users can regenerate their own in the new system)
*/
create or replace view migration_users as
select u.USER_ID     as SOURCE_USER_ID,
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
  and ar.REL_TYPE = 'membership_rel'
  and mr.MEMBER_STATE IS NOT NULL
order by u.user_id
