set @test_name = 0;
set @vl_qualitative = 0;
set @concept_id = 0;


-- HIV Viral Load construct
call ensure_concept(@concept_id, '11765b8c-a338-48a4-9480-df898c903723',
  'HIV viral load construct', 'HIV viral load construct', 'N/A', 'ConvSet',
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
  'HIV viral load, quantitative', 'HIV viral load numeric value', 'Numeric', 'Test', false);
call ensure_concept_set_members(@construct_name, @concept_id, 2);

-- HIV VIRAL LOAD, QUALITATIVE'
call ensure_concept(@vl_qualitative, '1305AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'HIV viral load, qualitative', 'HIV viral load, qualitative', 'Coded', 'Test',
  false);

set @concept_id = 0; 
call ensure_concept(@concept_id, '1306AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Beyond detectable limit', 'Beyond detectable limit', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 1);
  
set @concept_id = 0;  
call ensure_concept(@concept_id, '1304AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Poor sample quality', 'Poor sample quality', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 2);

set @concept_id = 0;  
call ensure_concept(@concept_id, '1301AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Detected', 'Detected', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 3);

set @concept_id = 0;  
call ensure_concept(@concept_id, '1302AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Not detected', 'Not detected', 'N/A', 'Misc', false);
call ensure_concept_answer(@vl_qualitative, @concept_id, 4);

call ensure_concept_set_members(@construct_name, @vl_qualitative, 3);

-- DETECTABLE LOWER LIMIT
set @concept_id = 0; 
call ensure_concept(@concept_id, '53cb83ed-5d55-4b63-922f-d6b8fc67a5f8',
  'Detectable lower limit', 'Detectable lower limit', 'Numeric', 'Misc', false);
call ensure_concept_set_members(@construct_name, @concept_id, 4);
