package org.pih.hivmigration.common.code;

/**
 * This corresponds to the hiv_exam_system_status.condition values
 */
public enum LabTest implements CodedValue {

	ABDOMINAL_ULTRASOUND,
	BIOCHEMISTRY,
	BUN,
	CD4,
	CHEST_XRAY,
	CREATININE,
	CULTURE,
	CXR,
	ELISA,
	ESR,
	GLUCOSE,
	HEMATOCRIT,
	HEMOGLOBIN,
	MALARIA_SMEAR,
	NONE,
	OTHER1,
	OTHER2,
	OTHER3,
	PLATELETS,
	PPD,
	PREGNANCY_TEST,
	RPR,
	SGOT_AST,
	SGPT_ALT,
	SMEAR,
	TB_SMEAR,
	VIRAL_LOAD,
	WESTERN_BLOT,
	WHITE_BLOOD_COUNT;

	public String getValue() {
		return name().toLowerCase();
	}
}
