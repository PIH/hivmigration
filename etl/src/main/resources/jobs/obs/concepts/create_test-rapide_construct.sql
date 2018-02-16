set @concept_id = 0;

-- HIV rapid test construct
call ensure_concept(@concept_id, 'A19F5A83-E960-413D-B93B-9270C53580A2',
  'HIV rapid test construct', 'HIV rapid test construct', 'N/A', 'ConvSet',
  true);
  set @construct_name = @concept_id;
  
-- Specimen Number
call ensure_concept(@concept_id, '162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Specimen number', 'Specimen number', 'Text', 'Question', false);
call ensure_concept_set_members(@construct_name, @concept_id, 1);
  
  -- HIV rapid test
call ensure_concept(@concept_id, '3cd6c946-26fe-102b-80cb-0017a47871b2',
  'HIV rapid test', 'HIV rapid test', 'Coded', 'Test', false);
set @hiv_rapid_test = @concept_id;

-- Positive
call ensure_concept(@concept_id, '3cd3a7a2-26fe-102b-80cb-0017a47871b2',
  'Positive', 'Positive', 'N/A', 'Misc', false);
call ensure_concept_answer(@hiv_rapid_test, @concept_id, 1);
-- Negative
 call ensure_concept(@concept_id, '3cd28732-26fe-102b-80cb-0017a47871b2',
  'Negative', 'Negative', 'N/A', 'Misc', false);
call ensure_concept_answer(@hiv_rapid_test, @concept_id, 2);
-- Indeterminate
call ensure_concept(@concept_id, '3cd774d6-26fe-102b-80cb-0017a47871b2',
  'Indeterminate', 'Indeterminate', 'N/A', 'Misc', false);
call ensure_concept_answer(@hiv_rapid_test, @concept_id, 3);
  
  
call ensure_concept_set_members(@construct_name, @hiv_rapid_test, 2);