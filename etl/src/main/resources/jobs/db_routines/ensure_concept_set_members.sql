CREATE PROCEDURE ensure_concept_set_members (
                                _set_concept_id INT,
                                _member_concept_id INT,
                                _sort_weight INT)
BEGIN
    DECLARE _concept_set_id INT;

    SELECT concept_set_id INTO _concept_set_id FROM concept_set
    WHERE concept_id = _member_concept_id AND concept_set = _set_concept_id;

    IF ( _concept_set_id IS NULL ) THEN
	    INSERT INTO concept_set (
	      concept_id,
	      concept_set,
	      sort_weight,
	      creator,
	      date_created,
	      uuid
	    )
	    values (
	      _member_concept_id,
	      _set_concept_id,
	      _sort_weight,
	      1,
	      now(),
	      uuid()
	    );
	ELSE
	    UPDATE concept_set SET sort_weight = _sort_weight
	    where concept_set_id = _concept_set_id;
	END IF;
END;