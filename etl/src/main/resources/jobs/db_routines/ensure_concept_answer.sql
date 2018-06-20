CREATE PROCEDURE ensure_concept_answer (
                                _concept_id INT,
                                _answer_concept_id INT,
                                _sort_weight DOUBLE)
BEGIN
    DECLARE _concept_answer_id INT;

    SELECT concept_answer_id INTO _concept_answer_id FROM concept_answer
    WHERE concept_id = _concept_id
    AND answer_concept = _answer_concept_id;

    IF ( _concept_answer_id IS NULL ) THEN
	    INSERT INTO concept_answer (
	      concept_id,
	      answer_concept,
	      answer_drug,
	      date_created,
	      creator,
	      uuid,
	      sort_weight)
	    values (
	      _concept_id,
	      _answer_concept_id,
	      null,
	      now(),
	      1,
	       uuid(),
	       _sort_weight);
	ELSE
	    UPDATE concept_answer SET sort_weight = _sort_weight
	    WHERE concept_answer_id = _concept_answer_id;
	END IF;
END;