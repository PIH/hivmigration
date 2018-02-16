set @test_name = 0;
set @exavir_id = 0;
set @vl_qualitative = 0;
set @concept_id = 0;


-- HIV Viral Load construct
call ensure_concept(@concept_id, '11765b8c-a338-48a4-9480-df898c903723',
  'HIV Viral Load  construct', 'HIV Viral Load  construct', 'N/A', 'ConvSet',
  true);
set @construct_name = @concept_id;
  
-- Specimen Number
set @concept_id = 0; 
call ensure_concept(@concept_id, '162086AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Specimen number', 'Specimen number', 'Text', 'Question', false);
call ensure_concept_set_members(@construct_name, @concept_id, 1);

-- HIV Viral Load Numeric Value
set @concept_id = 0; 
  call ensure_concept(@concept_id, '3cd4a882-26fe-102b-80cb-0017a47871b2',
  'HIV Viral Load', 'HIV Viral Load Numeric Value', 'Numeric', 'Test', false);
call ensure_concept_set_members(@construct_name, @concept_id, 2);

-- HIV VIRAL LOAD, QUALITATIVE'
call ensure_concept(@vl_qualitative, '1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'HIV VIRAL LOAD, QUALITATIVE', 'HIV VIRAL LOAD, QUALITATIVE', 'Coded', 'Test',
  false);

set @concept_id = 0; 
call ensure_concept(@concept_id, '1306AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'BEYOND DETECTABLE LIMIT', 'BEYOND DETECTABLE LIMIT', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 1);
  
set @concept_id = 0;  
call ensure_concept(@concept_id, '1304AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'POOR SAMPLE QUALITY', 'POOR SAMPLE QUALITY', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 2);

set @concept_id = 0;  
call ensure_concept(@concept_id, '1301AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'DETECTED', 'DETECTED', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 3);

set @concept_id = 0;  
call ensure_concept(@concept_id, '1302AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'NOT DETECTED', 'NOT DETECTED', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 4);

call ensure_concept_set_members(@construct_name, @vl_qualitative, 3);

-- DETECTABLE LOWER LIMIT
set @concept_id = 0; 
call ensure_concept(@concept_id, '53cb83ed-5d55-4b63-922f-d6b8fc67a5f8',
  'Detectable lower limit', 'Detectable lower limit', 'Numeric', 'Misc', false);
call ensure_concept_set_members(@construct_name, @concept_id, 4);
  
-- Test Name
call ensure_concept(@test_name, '162087AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Test name', 'Test name', 'Coded', 'Question',
  false);
  
  call ensure_concept(@exavir_id, '6ECB7B1A-7010-4D29-8DBE-E883C2179068',
  'ExaVir', 'ExaVir', 'N/A', 'Misc', false);
call ensure_concept_answer(@test_name, @exavir_id, 1);

call ensure_concept_set_members(@construct_name, @test_name, 5);


