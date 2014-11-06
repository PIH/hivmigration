package org.pih.hivmigration.common.code;

/**
 * This corresponds to the hiv_exam_system_status.system values
 */
public enum BodySystem implements CodedValue {

	LYMPH_NODES,
	CONJUNCTIVA,
	ABDOMINAL,
	UROGENITAL,
	SCLERAS,
	EXTREMITIES,
	GENERAL,
	SKIN,
	NEUROLOGIC,
	OROPHARYNX,
	CARDIOVASCULAR,
	PULMONARY;

	public String getValue() {
		return name().toLowerCase();
	}
}
