package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to the hiv_exam_symptoms.symptom column
 */
public enum SymptomConcept implements CodedValue {

	AUCUN,
	BLOODY_DIARRHEA,
	CEPHALGIA,
	CHEST_PAIN,
	CONFUSION,
	CONVULSIONS,
	COUGH,
	DIARRHEA,
	DIARRHEA_SHORT,
	DRY_COUGH,
	DYSPHAGIA,
	DYSPNEA,
	EYE_TROUBLE,
	FATIGUE,
	FEVER,
	FOCAL_NEUROLOGICAL_DEFICIT,
	GENITAL_DISCHARGE,
	GENITAL_ULCERS,
	HEADACHE,
	HYMOPTUSIS,
	ICTERUS,
	JAUNDICE,
	LOSS_OF_WEIGHT,
	LYMPHADENOPATHY,
	NAUSEA,
	NEURO,
	NEUROLOGIC_DEFICIT,
	NIGHT_SWEATS,
	OTHER,
	PARESTHESIA,
	PRODUCTIVE_COUGH,
	PRURIGO_NODULARIS,
	PRURITUS,
	PURITUS,
	RASH,
	SEIZURES,
	SHINGLES,
	STI,
	TB_CONTACT,
	THRUSH,
	VISION_PROBLEMS,
	VOMITING;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
