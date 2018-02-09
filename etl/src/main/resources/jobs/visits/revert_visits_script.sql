
delete from visit where uuid in (select visit_uuid from hivmigration_visits);
