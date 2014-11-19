package org.pih.hivmigration.common.code;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This corresponds to coded lab tests, either from orders or results
 */
public enum LabTest implements CodedValue {

	ABDOMINAL_ULTRASOUND,
	BIOCHEMISTRY,
	BLOOD_GLUCOSE,
	BLOOD_TYPE,
	BUN,
	CD4,
	CD4_PERCENT,
	CHEST_XRAY,
	CHOLESTEROL,
	CREATININE,
	CULTURE,
	CXR,
	CXR0,
	CXR1,
	CXR2,
	CYTOLOGY,
	DST,
	ELISA,
	ESR,
	GLUCOSE,
	ERYTH_SED_RATE,
	HANGING_DROP,
	HEMATOCRIT("hematocrite"),
	HEMOGLOBIN,
	MALARIA_SMEAR,
	NONE,
	OTHER,
	OTHER1,
	OTHER2,
	OTHER3,
	OTHER_RADIOLOGY,
	OTHER_STD,
	OTHER_TEST,
	PLATELETS,
	PPD, // This is a unique case that can have one or both of valueCoded and valueNumeric non-null, representing postive/negative and size in mm
	PREGNANCY_TEST,
	RPR,
	SGOT,
	SGOT_AST,
	SGPT,
	SGPT_ALT,
	SMEAR,
	SMEAR1,
	SMEAR2,
	SMEAR3,
	SPUTUM,
	STOOL,
	TB_SMEAR,
	TOT_LYMPH_COUNT,
	TOT_LYMPH_COUNT_PERCENT, // This reflects TOT_LYMPH_COUNT results stored as percentages
	TR,
	UREA,
	URINALYSIS,
	VAGINAL_SMEAR,
	VIRAL_LOAD,
	WESTERN_BLOT,
	WHITE_BLOOD_COUNT;

	private List<String> synonyms;

	LabTest() {}

	LabTest(String...synonyms) {
		this.synonyms = Arrays.asList(synonyms);
	}

	@Override
	public List<String> getValues() {
		List<String> ret = new ArrayList<String>();
		ret.add(name().toLowerCase());
		if (synonyms != null) {
			ret.addAll(synonyms);
		}
		return ret;
	}
}
