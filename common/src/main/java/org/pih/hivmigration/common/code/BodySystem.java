package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

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

	@Override
	public List<String> getValues() {
		return Arrays.asList(name().toLowerCase());
	}
}
