CREATE PROCEDURE add_patient_program(
	_patient_id INT,
	_hiv_program_id INT,
	_enrollment_date DATE,
	_program_outcome_date DATE,
	_art_start_date DATE,
	_location_id INT,
	_patient_outcome INT,
	_creator INT
)
BEGIN

	DECLARE on_art_start_date date;

	SET @on_ART_state = 1;
    SET @pre_ART_state = 2;

	INSERT INTO patient_program(patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id,
			creator, date_created, uuid)
	VALUES(_patient_id, _hiv_program_id, _enrollment_date, _program_outcome_date, _location_id, _patient_outcome,
			_creator, curdate(), uuid());
    SET @patient_program_id = (SELECT LAST_INSERT_ID());


	IF ( (_art_start_date is not null) and (_art_start_date > _enrollment_date)) THEN
		INSERT INTO patient_state(patient_program_id, state, start_date, end_date, creator, date_created, uuid)
		VALUES(@patient_program_id, @pre_ART_state, _enrollment_date, _art_start_date, _creator, curdate(), uuid());
        SET on_art_start_date = _art_start_date;
	ELSE
		SET on_art_start_date = _enrollment_date;
	END IF;


	INSERT INTO patient_state(patient_program_id, state, start_date, end_date, creator, date_created, uuid)
	VALUES(@patient_program_id, @on_ART_state, on_art_start_date, _program_outcome_date, _creator, curdate(), uuid());
END
