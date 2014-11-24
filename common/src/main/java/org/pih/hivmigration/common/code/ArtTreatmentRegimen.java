package org.pih.hivmigration.common.code;

import java.util.Arrays;
import java.util.List;

/**
 * This corresponds an observation recording of the patients current art regimen
 */
public enum ArtTreatmentRegimen implements CodedValue {

	NONE,
	AZT_3TC_EFV,
	AZT_3TC_NVP,
	D4T_3TC_EFV,
	D4T_3TC_NVP;

	@Override
	public List<String> getValues() {
		return Arrays.asList(name());
	}
}
