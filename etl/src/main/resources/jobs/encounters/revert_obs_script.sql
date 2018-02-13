delete from obs where obs_group_id in (select obs_id from hivmigration_lab_results);
delete from obs where obs_id in (select obs_id from hivmigration_lab_results);