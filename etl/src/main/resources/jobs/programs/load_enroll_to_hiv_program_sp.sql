CREATE PROCEDURE enroll_to_hiv_program()
BEGIN
    DECLARE _finished INT;
	DECLARE v_source_patient_id int;
	DECLARE v_health_center_id int;
	DECLARE v_enrollment_date date;
	DECLARE v_art_start_date date;
	DECLARE v_outcome_date date;
	DECLARE v_outcome varchar(32);
	DECLARE patient_id int;
    DECLARE patient_outcome int;
	DECLARE program_outcome_date date;
	DECLARE on_art_start_date date;

	DECLARE program_cursor CURSOR FOR
		select source_patient_id, location_id, enrollment_date, art_start_date, outcome_date, outcome
		from hivmigration_programs;

	-- declare NOT FOUND handler
	DECLARE CONTINUE HANDLER
		FOR NOT FOUND SET _finished = 1;

    SET @creator = 1;
	SET @hiv_program_id = 2; -- HIV Program ID
	SET @on_ART_state = 1; -- On_ART HIV Program state
    SET @pre_ART_state = 2; -- On_ART HIV Program state
	SET @outcome_died = (select concept_id from concept where uuid = '3cdd446a-26fe-102b-80cb-0017a47871b2');
	SET @outcome_lost_to_followup = (select concept_id from concept where uuid = '3ceb0ed8-26fe-102b-80cb-0017a47871b2');
	SET @outcome_transferred_out = (select concept_id from concept where uuid = '3cdd5c02-26fe-102b-80cb-0017a47871b2');
	SET @outcome_treatment_stopped = (select concept_id from concept where uuid = '3cdc0d7a-26fe-102b-80cb-0017a47871b2');

	OPEN program_cursor;

	get_patient: LOOP

	FETCH program_cursor INTO v_source_patient_id, v_health_center_id, v_enrollment_date, v_art_start_date, v_outcome_date, v_outcome;

	IF _finished = 1 THEN
	 LEAVE get_patient;
	END IF;

	SET patient_id = (select person_id from hivmigration_patients where source_patient_id = v_source_patient_id);
	-- map the HIV Healthcenter ID to OpenMRS Location ID
	SET @location_id = (select location_id from hivmigration_health_center where health_center_id = v_health_center_id);

    IF v_outcome_date is not null THEN
		SET program_outcome_date = v_outcome_date;
	ELSE
		SET program_outcome_date = null;
	END IF;

    CASE v_outcome
		WHEN 'ABANDONED' THEN
			SET patient_outcome = @outcome_lost_to_followup;
		WHEN 'DIED' THEN
			SET patient_outcome = @outcome_died;
		WHEN 'TRANSFERRED_OUT' THEN
			SET patient_outcome = @outcome_transferred_out;
		WHEN 'TREATMENT_STOPPED' THEN
			SET patient_outcome = @outcome_treatment_stopped;
        ELSE
			SET patient_outcome = null;
	END CASE;

	-- SELECT v_source_patient_id, v_health_center_id, @location_id, v_outcome, patient_outcome, program_outcome_date;

	INSERT INTO patient_program(patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id,
			creator, date_created, uuid)
	VALUES(patient_id, @hiv_program_id, v_enrollment_date, program_outcome_date, @location_id, patient_outcome,
			@creator, curdate(), uuid());
    SET @patient_program_id = (SELECT LAST_INSERT_ID());

	IF ( (v_art_start_date is not null) and (v_art_start_date > v_enrollment_date)) THEN
		   -- create PRE_ART state
		INSERT INTO patient_state(patient_program_id, state, start_date, end_date, creator, date_created, uuid)
		VALUES(@patient_program_id, @pre_ART_state, v_enrollment_date, v_art_start_date, @creator, curdate(), uuid());
        SET on_art_start_date = v_art_start_date;
	ELSE
		SET on_art_start_date = v_enrollment_date;
	END IF;

	-- create ON_ART state
	INSERT INTO patient_state(patient_program_id, state, start_date, end_date, creator, date_created, uuid)
	VALUES(@patient_program_id, @on_ART_state, on_art_start_date, program_outcome_date, @creator, curdate(), uuid());

	END LOOP get_patient;

END