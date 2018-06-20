CREATE PROCEDURE ensure_concept (
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
END;


