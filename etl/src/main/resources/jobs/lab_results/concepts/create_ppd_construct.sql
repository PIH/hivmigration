set @concept_id = 0;

-- PPD construct
call ensure_concept(@concept_id, 'ACF90DED-B595-4356-9840-788094C60AFB',
  'PPD construct', 'PPD construct', 'N/A', 'ConvSet',
  true);
  set @construct_name = @concept_id;
  
-- Specimen Number
call ensure_concept(@concept_id, '162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Specimen number', 'Specimen number', 'Text', 'Question', false);
call ensure_concept_set_members(@construct_name, @concept_id, 1);
  
  -- PPD
  call ensure_concept(@concept_id, '3cecf388-26fe-102b-80cb-0017a47871b2',
  'PPD', 'PPD', 'Numeric', 'Test', false);
  
call ensure_concept_set_members(@construct_name, @concept_id, 2);