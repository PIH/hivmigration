set @concept_id = 0;

call ensure_concept(@concept_id, '164141AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
  'Name of community health worker', 'Name of community health worker', 'Text', 'Question', false);

call ensure_concept(@concept_id, '6a38b794-7a49-11e8-8624-54ee75ef41c2',
  'Medication Picked Up By Patient', 'Medication Picked Up By Patient', 'Boolean', 'Question', false);

call ensure_concept(@concept_id, '7af8ecdd-7a49-11e8-8624-54ee75ef41c2',
  'Medication Pickup Date', 'Medication Pickup Date', 'Datetime', 'Question', false);

call ensure_concept(@concept_id, '8266b6fb-7a49-11e8-8624-54ee75ef41c2',
  'Medication Pickup Date Of Next Visit', 'Medication Pickup Date Of Next Visit', 'Datetime', 'Question', false);

call ensure_concept(@concept_id, '8b0472fc-7a49-11e8-8624-54ee75ef41c2',
  'Medication Picked Up Ctx', 'Medication Picked Up Ctx', 'Boolean', 'Question', false);

call ensure_concept(@concept_id, '927e5248-7a49-11e8-8624-54ee75ef41c2',
  'Medication Picked Up Inh', 'Medication Picked Up Inh', 'Boolean', 'Question', false);

call ensure_concept(@concept_id, '99771a33-7a49-11e8-8624-54ee75ef41c2',
  'Medication Pickup Reason For Not Coming', 'Medication Pickup Reason For Not Coming', 'Text', 'Question', false);


-- common true/false concepts
call ensure_concept(@concept_id, '3ce22110-26fe-102b-80cb-0017a47871b2', 'TRUE', 'True', 'N/A', 'Misc', false);
call ensure_concept(@concept_id, '3ce22c50-26fe-102b-80cb-0017a47871b2', 'FALSE', 'False', 'N/A', 'Misc', false);
