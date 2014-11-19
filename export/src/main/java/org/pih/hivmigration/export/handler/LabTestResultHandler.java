package org.pih.hivmigration.export.handler;

import org.pih.hivmigration.common.LabTestResult;
import org.pih.hivmigration.common.code.BacteriologyResult;
import org.pih.hivmigration.common.code.BloodType;
import org.pih.hivmigration.common.code.CodedValue;
import org.pih.hivmigration.common.code.CytologyResult;
import org.pih.hivmigration.common.code.DstResult;
import org.pih.hivmigration.common.code.LabTest;
import org.pih.hivmigration.common.code.SimpleLabResult;
import org.pih.hivmigration.common.util.Util;
import org.pih.hivmigration.export.ExportUtil;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LabTestResultHandler {

	public static List<LabTest> TEXT_RESULTS = Arrays.asList(
		LabTest.ABDOMINAL_ULTRASOUND,
		LabTest.CHEST_XRAY,
		LabTest.CULTURE, // This has only 51 results, and while "negative" variations are most common, broad range of other options too
		LabTest.CXR,
		LabTest.CXR0,
		LabTest.CXR1,
		LabTest.CXR2,
		LabTest.HANGING_DROP,
		LabTest.OTHER,
		LabTest.OTHER_RADIOLOGY,
		LabTest.OTHER_STD,
		LabTest.OTHER_TEST,
		LabTest.STOOL,
		LabTest.URINALYSIS,
		LabTest.VAGINAL_SMEAR
	);

	public static List<LabTest> NUMERIC_RESULTS = Arrays.asList(
		LabTest.BLOOD_GLUCOSE, // 4 exceptional values exist
		LabTest.CHOLESTEROL, // 1 exceptional values exist
		LabTest.CREATININE, // 25 exceptional values exist
		LabTest.ERYTH_SED_RATE, // 52 exceptional values exist
		LabTest.SGOT, // 13 exceptional values exist
		LabTest.SGPT, // 29 exceptional values exist
		LabTest.UREA, // 13 exceptional values exist
		LabTest.VIRAL_LOAD // 9 exceptional values exist (only 13 numeric too)
	);

	public static Map<LabTest, Class<? extends CodedValue>> CODED_RESULTS = Util.toMap(
		LabTest.BLOOD_TYPE, BloodType.class,
		LabTest.CYTOLOGY, CytologyResult.class,
		LabTest.DST, DstResult.class,
		LabTest.ELISA, SimpleLabResult.class, // 1 other value (non fait)
		LabTest.RPR, SimpleLabResult.class, // ~30 other values
		LabTest.SMEAR1, BacteriologyResult.class,
		LabTest.SMEAR2, BacteriologyResult.class,
		LabTest.SMEAR3, BacteriologyResult.class,
		LabTest.SPUTUM, BacteriologyResult.class, // There are quite a few exceptional values here
		LabTest.TR, SimpleLabResult.class, // ~45 other values
		LabTest.WESTERN_BLOT, SimpleLabResult.class // only one result, positive, in the data as of now
	);

	public static List<LabTest> PERCENTAGE_RESULTS = Arrays.asList(
		LabTest.CD4_PERCENT, // Exceptional values exist, including several values over 100 that are likely just CD4s
		LabTest.HEMATOCRIT
	);

	public static Map<LabTest, Class<? extends CodedValue>> NUMERIC_OR_PERCENTAGE_RESULTS = Util.toMap(
		LabTest.CD4, LabTest.CD4_PERCENT,
		LabTest.HEMOGLOBIN, LabTest.HEMATOCRIT,
		LabTest.TOT_LYMPH_COUNT, LabTest.TOT_LYMPH_COUNT_PERCENT
	);

	public static List<LabTest> PPD_RESULTS = Arrays.asList(
		LabTest.PPD // Needs to try to split positive/negative and numeric mm measurement into 2 different obs, plus many exceptional values
	);

	public static LabTestResult createLabTestResultResult(Map<String, Object> dataRow) {

		Object encounterId = dataRow.get("ENCOUNTER_ID");
		LabTest labTest = ExportUtil.convertValue(dataRow.get("LAB_TEST"), LabTest.class);
		Date testDate = ExportUtil.convertValue(dataRow.get("TEST_DATE"), Date.class);
		String sampleId = (String)dataRow.get("SAMPLE_ID");
		String result = (String)dataRow.get("RESULT");
		BigDecimal resultNumeric = ExportUtil.convertValue(dataRow.get("VALUE_NUMERIC"), BigDecimal.class);
		SimpleLabResult resultBoolean = ExportUtil.convertValue(dataRow.get("VALUE_BOOLEAN"), SimpleLabResult.class);
		String resultText = (String)dataRow.get("VALUE_TEXT");

		if (encounterId == null || labTest == null) {
			throw new IllegalArgumentException("Missing either encounterId, testName, or testDate: " + dataRow);
		}

		boolean hasExamResult = result != null;
		boolean hasLabResult = (resultNumeric != null || resultBoolean != null || resultText != null);
		if (hasExamResult && hasLabResult) {
			throw new IllegalArgumentException("Expected either exam result or lab result: " + dataRow);
		}
		if (!hasExamResult && !hasLabResult) {
			throw new IllegalArgumentException("Missing result: " + dataRow);
		}

		LabTestResult ret = new LabTestResult(labTest, testDate);
		ret.setSampleId(sampleId);

		if (resultNumeric != null) {
			ret.setValueNumeric(resultNumeric.doubleValue());
		}
		else if (resultBoolean != null) {
			ret.setValueCoded(resultBoolean);
		}
		else if (resultText != null || result != null) {
			String rawResult = Util.nvlStr(resultText, result);
			if (Util.isEmpty(rawResult)) {
				throw new IllegalArgumentException("Result is empty: " + dataRow);
			}
			if (TEXT_RESULTS.contains(labTest)) {
				ret.setValueText(rawResult);
			}
			else if (NUMERIC_RESULTS.contains(labTest)) {
				setNumericValue(ret, rawResult);
			}
			else if (CODED_RESULTS.containsKey(labTest)) {
				setCodedValue(ret, rawResult, CODED_RESULTS.get(labTest));
			}
			else if (PERCENTAGE_RESULTS.contains(labTest)) {
				setPercentageValue(ret, rawResult);
			}
			else if (NUMERIC_OR_PERCENTAGE_RESULTS.containsKey(labTest)) {
				if (rawResult.contains("%")) {
					setPercentageValue(ret, rawResult);
				}
				else {
					setNumericValue(ret, rawResult);
				}
			}
			else if (PPD_RESULTS.contains(labTest)) {
				String[] ppdSplit = rawResult.split(":");
				if (ppdSplit.length == 1) {
					SimpleLabResult codedResult = ExportUtil.getCodedValue(SimpleLabResult.class, rawResult);
					if (codedResult != null) {
						setCodedValue(ret, rawResult, SimpleLabResult.class);
					}
					else {
						setNumericValue(ret, rawResult);
					}
				}
				else {
					SimpleLabResult codedResult = ExportUtil.getCodedValue(SimpleLabResult.class, ppdSplit[0]);
					Double numericResult = Util.parseDoubleIfPossible(ppdSplit[1]);
					if (codedResult != null && numericResult != null) {
						ret.setValueCoded(codedResult);
						ret.setValueNumeric(numericResult);
					}
					else {
						ret.setValueException(rawResult);
					}
				}
			}
			else {
				throw new IllegalArgumentException("Uncategorized test found: " + labTest);
			}
		}

		return ret;
	}

	public static void setNumericValue(LabTestResult r, String rawResult) {
		try {
			r.setValueNumeric(Double.parseDouble(rawResult));
		}
		catch (Exception e) {
			r.setValueException(rawResult);
		}
	}

	public static void setCodedValue(LabTestResult r, String rawResult, Class<? extends CodedValue> codedValueType) {
		CodedValue cv = ExportUtil.getCodedValue(codedValueType, rawResult);
		if (cv != null) {
			r.setValueCoded(cv);
		}
		else {
			r.setValueException(rawResult);
		}
	}

	public static void setPercentageValue(LabTestResult r, String rawResult) {
		String percentValue = rawResult.replace("%", "").trim();
		try {
			Double pct = Double.parseDouble(percentValue);
			if (pct < 100) {
				r.setValuePercent(pct);
			}
			else {
				r.setValueException(rawResult);
			}
		}
		catch (Exception e) {
			r.setValueException(rawResult);
		}
	}
}
