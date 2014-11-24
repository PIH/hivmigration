package org.pih.hivmigration.common.code;

import java.util.ArrayList;
import java.util.List;

/**
 * This corresponds an observation recording of the patients current TB regimen
 */
public enum TbTreatmentRegimen implements CodedValue {

	NONE,
	MDR_TB_TREATMENT,
	TB_INFANT_2HRZ,
	TB_INFANT_4HR,
	TB_INITIAL_2HRZE,
	TB_INITIAL_4HR,
	TB_RETREATMENT_1HRZE,
	TB_RETREATMENT_2S_PLUS_HRZE,
	TB_RETREATMENT_5HR_PLUS_E;

	@Override
	public List<String> getValues() {
		List<String> l = new ArrayList<String>();
		l.add(name());
		l.add(name().replace("_PLUS_", "+"));
		return l;
	}
}
