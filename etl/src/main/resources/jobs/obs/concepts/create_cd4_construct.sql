set @concept_id = 0;

-- CD4 Count construct
call ensure_concept(@concept_id, '37769FDB-5FC1-4D47-82C2-DB88960BB224',
  'CD4 Count construct', 'CD4 Count construct', 'N/A', 'ConvSet',
  true);
  set @construct_name = @concept_id;
  
-- Specimen Number
call ensure_concept(@concept_id, '162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Specimen number', 'Specimen number', 'Text', 'Question', false);
call ensure_concept_set_members(@construct_name, @concept_id, 1);
  
  -- CD4 Count
  call ensure_concept(@concept_id, '3ceda710-26fe-102b-80cb-0017a47871b2',
  'CD4 count', 'CD4 count', 'Numeric', 'Test', false);
  
call ensure_concept_set_members(@construct_name, @concept_id, 2);