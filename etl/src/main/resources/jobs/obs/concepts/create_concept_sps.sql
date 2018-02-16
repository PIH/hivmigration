DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `ensure_concept`(
INOUT new_concept_id INT,
                                _concept_uuid CHAR(38),
                                concept_name VARCHAR(255),
                                concept_short_name VARCHAR(255),
                                data_type_name VARCHAR(255),
                                class_name VARCHAR(255),
                                is_set BOOLEAN
)
BEGIN
DECLARE data_type_id INT;
  DECLARE class_id INT;
  DECLARE is_set_val TINYINT(1);
  DECLARE _concept_id INT;

  CASE
    WHEN is_set = TRUE THEN
       SET is_set_val = '1';
    WHEN is_set = FALSE THEN
       SET is_set_val = '0';
  END CASE;

  SELECT concept_id INTO _concept_id FROM concept WHERE uuid = _concept_uuid;

  IF ( _concept_id IS NULL ) THEN

      SELECT concept_datatype_id INTO data_type_id FROM concept_datatype WHERE name = data_type_name;
      SELECT concept_class_id INTO class_id FROM concept_class WHERE name = class_name;

      INSERT INTO concept (datatype_id, class_id, is_set, creator, date_created, changed_by, date_changed, uuid)
        values (data_type_id, class_id, is_set_val, 1, now(), 1, now(), _concept_uuid);
      SELECT MAX(concept_id) INTO _concept_id FROM concept;

      INSERT INTO concept_name (concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, uuid)
        values (_concept_id, concept_short_name, 'en', 0, 1, now(), 'SHORT', uuid());

      INSERT INTO concept_name (concept_id, name, locale, locale_preferred, creator, date_created, concept_name_type, uuid)
        values (_concept_id, concept_name, 'en', 1, 1, now(), 'FULLY_SPECIFIED', uuid());

  END IF;

  SET new_concept_id = _concept_id;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `ensure_concept_answer`(_concept_id INT,
                                _answer_concept_id INT,
                                _sort_weight DOUBLE)
BEGIN
  DECLARE _concept_answer_id INT;

    SELECT concept_answer_id INTO _concept_answer_id FROM concept_answer WHERE concept_id = _concept_id AND answer_concept = _answer_concept_id;

    IF ( _concept_answer_id IS NULL ) THEN
	    INSERT INTO concept_answer (concept_id, answer_concept, answer_drug, date_created, creator, uuid, sort_weight) values (_concept_id, _answer_concept_id, null, now(), 1, uuid(), _sort_weight);
	ELSE
	    UPDATE concept_answer SET sort_weight = _sort_weight WHERE concept_answer_id = _concept_answer_id;
	END IF;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `ensure_concept_set_members`(
								_set_concept_id INT,
                                _member_concept_id INT,
                                _sort_weight INT)
BEGIN
 DECLARE _concept_set_id INT;

    SELECT concept_set_id INTO _concept_set_id FROM concept_set WHERE concept_id = _member_concept_id AND concept_set = _set_concept_id;

    IF ( _concept_set_id IS NULL ) THEN
	    INSERT INTO concept_set (concept_id, concept_set, sort_weight, creator, date_created, uuid) values (_member_concept_id, _set_concept_id, _sort_weight, 1, now(), uuid());
	ELSE
	    UPDATE concept_set SET sort_weight = _sort_weight where concept_set_id = _concept_set_id;
	END IF;
END$$
DELIMITER ;
