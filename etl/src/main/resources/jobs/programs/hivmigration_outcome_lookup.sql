drop table if exists hivmigration_outcome;

create table hivmigration_outcome (
    id int PRIMARY KEY AUTO_INCREMENT,
	outcome varchar(255),
	outcome_concept_id int
);

SET @outcome_died = (select concept_id from concept where uuid = '3cdd446a-26fe-102b-80cb-0017a47871b2');
SET @outcome_lost_to_followup = (select concept_id from concept where uuid = '3ceb0ed8-26fe-102b-80cb-0017a47871b2');
SET @outcome_transferred_out = (select concept_id from concept where uuid = '3cdd5c02-26fe-102b-80cb-0017a47871b2');
SET @outcome_treatment_stopped = (select concept_id from concept where uuid = '3cdc0d7a-26fe-102b-80cb-0017a47871b2');

insert into hivmigration_outcome(outcome, outcome_concept_id) values("DIED", @outcome_died);
insert into hivmigration_outcome(outcome, outcome_concept_id) values("ABANDONED", @outcome_lost_to_followup);
insert into hivmigration_outcome(outcome, outcome_concept_id) values("TREATMENT_STOPPED", @outcome_treatment_stopped);
insert into hivmigration_outcome(outcome, outcome_concept_id) values("TRANSFERRED_OUT", @outcome_transferred_out);
insert into hivmigration_outcome(outcome, outcome_concept_id) values("TRANSFERRED_INTERNALLY", @outcome_transferred_out);

