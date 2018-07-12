set @concept_id = 0;

call ensure_concept(@concept_id, '3ce93cf2-26fe-102b-80cb-0017a47871b2',
  'Height (cm)', 'HT', 'Numeric', 'Test', false);

call ensure_concept(@concept_id, '3ce93b62-26fe-102b-80cb-0017a47871b2',
  'Weight (kg)', 'WT', 'Numeric', 'Test', false);

call ensure_concept(@concept_id, '3ce14da8-26fe-102b-80cb-0017a47871b2',
  'BODY MASS INDEX, MEASURED', 'BMI', 'Numeric', 'Question', false);

