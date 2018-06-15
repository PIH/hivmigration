set @concept_id = 0;

-- Hematocrit construct
call ensure_concept(@concept_id, '267C165C-1B8F-48FE-91AC-C1AE8C7412A0',
  'Hematocrit construct', 'Hematocrit construct', 'N/A', 'ConvSet',
  true);
  set @construct_name = @concept_id;
  
-- Specimen Number
call ensure_concept(@concept_id, '162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Specimen number', 'Specimen number', 'Text', 'Question', false);
call ensure_concept_set_members(@construct_name, @concept_id, 1);
  
  -- Hematocrit
  call ensure_concept(@concept_id, '3cd69a98-26fe-102b-80cb-0017a47871b2',
  'Hematocrit', 'Hematocrit', 'Numeric', 'Test', false);
  
call ensure_concept_set_members(@construct_name, @concept_id, 2);