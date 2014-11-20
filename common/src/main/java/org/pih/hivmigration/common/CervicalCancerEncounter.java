package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.CytologyPlan;
import org.pih.hivmigration.common.code.SimpleLabResult;

import java.util.Date;
import java.util.List;

/**
 * Represents a cervical cancer screening encounter
 */
public class CervicalCancerEncounter extends Encounter {

	private String location;  // TODO: Make this a Location reference
	private Date lastPeriodDate;
	private List<LabTestResult> labResults;

	@ObsName("agent_p") private Boolean hasAccompagnateur;
	@ObsName("breast_exam") private String breastExam;
	@ObsName("cryotherapy_performed_today_p") private Boolean cryotherapyPerformedToday;
	@ObsName("cytology_plan") private CytologyPlan cytologyPlan;
	@ObsName("dysmenorrhea") private Boolean dysmenorrhea;
	@ObsName("dysparuenia_p") private Boolean dysparuenia;
	@ObsName("family_planning_used_p") private Boolean familyPlanningUsed;
	@ObsName("family_planning_method") private List<String> familyPlanningMethod;
	@ObsName("family_planning_method_since_date") private Date familyPlanningMethodSinceDate;
	@ObsName("gravidity") private Integer gravidity;
	@ObsName("parity") private Integer parity;
	@ObsName("num_abortions") private Integer numAbortions;
	@ObsName("num_living_children") private Integer numLivingChildren;
	@ObsName("irregular_bleeding_p") private Boolean irregularBleeding;
	@ObsName("irregular_bleeding_comments") private String irregularBleedingComments;
	@ObsName("lesion_appropriate_for_cryotherapy_p") private Boolean lesionAppropriateForCryotherapy;
	@ObsName("lesion_appropriate_for_cryotherapy_comment") private String lesionAppropriateForCryotherapyComment;
	@ObsName("menarche") private Integer menarche;
	@ObsName("regular_menses") private Boolean regularMenses;
	@ObsName("menses_days_apart") private String mensesDaysApart;
	@ObsName("menses_days_duration") private Integer mensesDaysDuration;
	@ObsName("name_of_provider_who_performed_via") private String nameOfProviderWhoPerformedVia;
	@ObsName("name_of_provider_who_reviewed_cytology") private String nameOfProviderWhoReviewedCytology;
	@ObsName("next_cytology_visit_weeks") private Integer nextCytologyVisitWeeks;
	@ObsName("next_visit_weeks") private Integer nextVisitWeeks;
	@ObsName("pap_test_today_p") private Boolean papTestToday;
	@ObsName("post_coital_bleeding_p") private Boolean postCoitalBleeding;
	@ObsName("referred_for_colposcopy_p") private Boolean referredForColposcopy;
	@ObsName("referred_for_evaluation_with_gyn") private Boolean referredForEvaluationWithGyn;
	@ObsName("self_breast_exam_taught_p") private Boolean selfBreastExamTaught;
	@ObsName("vaginal_discharge_p") private Boolean vaginalDischarge;
	@ObsName("vaginal_itching_burning") private Boolean vaginalItchingBurning;
	@ObsName("via_cervix_result") private String viaCervixResult;
	@ObsName("via_results") private SimpleLabResult viaResults;

	public CervicalCancerEncounter() {}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public Date getLastPeriodDate() {
		return lastPeriodDate;
	}

	public void setLastPeriodDate(Date lastPeriodDate) {
		this.lastPeriodDate = lastPeriodDate;
	}

	public List<LabTestResult> getLabResults() {
		return labResults;
	}

	public void setLabResults(List<LabTestResult> labResults) {
		this.labResults = labResults;
	}

	public Boolean getHasAccompagnateur() {
		return hasAccompagnateur;
	}

	public void setHasAccompagnateur(Boolean hasAccompagnateur) {
		this.hasAccompagnateur = hasAccompagnateur;
	}

	public String getBreastExam() {
		return breastExam;
	}

	public void setBreastExam(String breastExam) {
		this.breastExam = breastExam;
	}

	public Boolean getCryotherapyPerformedToday() {
		return cryotherapyPerformedToday;
	}

	public void setCryotherapyPerformedToday(Boolean cryotherapyPerformedToday) {
		this.cryotherapyPerformedToday = cryotherapyPerformedToday;
	}

	public CytologyPlan getCytologyPlan() {
		return cytologyPlan;
	}

	public void setCytologyPlan(CytologyPlan cytologyPlan) {
		this.cytologyPlan = cytologyPlan;
	}

	public Boolean getDysmenorrhea() {
		return dysmenorrhea;
	}

	public void setDysmenorrhea(Boolean dysmenorrhea) {
		this.dysmenorrhea = dysmenorrhea;
	}

	public Boolean getDysparuenia() {
		return dysparuenia;
	}

	public void setDysparuenia(Boolean dysparuenia) {
		this.dysparuenia = dysparuenia;
	}

	public Boolean getFamilyPlanningUsed() {
		return familyPlanningUsed;
	}

	public void setFamilyPlanningUsed(Boolean familyPlanningUsed) {
		this.familyPlanningUsed = familyPlanningUsed;
	}

	public List<String> getFamilyPlanningMethod() {
		return familyPlanningMethod;
	}

	public void setFamilyPlanningMethod(List<String> familyPlanningMethod) {
		this.familyPlanningMethod = familyPlanningMethod;
	}

	public Date getFamilyPlanningMethodSinceDate() {
		return familyPlanningMethodSinceDate;
	}

	public void setFamilyPlanningMethodSinceDate(Date familyPlanningMethodSinceDate) {
		this.familyPlanningMethodSinceDate = familyPlanningMethodSinceDate;
	}

	public Integer getGravidity() {
		return gravidity;
	}

	public void setGravidity(Integer gravidity) {
		this.gravidity = gravidity;
	}

	public Integer getParity() {
		return parity;
	}

	public void setParity(Integer parity) {
		this.parity = parity;
	}

	public Integer getNumAbortions() {
		return numAbortions;
	}

	public void setNumAbortions(Integer numAbortions) {
		this.numAbortions = numAbortions;
	}

	public Integer getNumLivingChildren() {
		return numLivingChildren;
	}

	public void setNumLivingChildren(Integer numLivingChildren) {
		this.numLivingChildren = numLivingChildren;
	}

	public Boolean getIrregularBleeding() {
		return irregularBleeding;
	}

	public void setIrregularBleeding(Boolean irregularBleeding) {
		this.irregularBleeding = irregularBleeding;
	}

	public String getIrregularBleedingComments() {
		return irregularBleedingComments;
	}

	public void setIrregularBleedingComments(String irregularBleedingComments) {
		this.irregularBleedingComments = irregularBleedingComments;
	}

	public Boolean getLesionAppropriateForCryotherapy() {
		return lesionAppropriateForCryotherapy;
	}

	public void setLesionAppropriateForCryotherapy(Boolean lesionAppropriateForCryotherapy) {
		this.lesionAppropriateForCryotherapy = lesionAppropriateForCryotherapy;
	}

	public String getLesionAppropriateForCryotherapyComment() {
		return lesionAppropriateForCryotherapyComment;
	}

	public void setLesionAppropriateForCryotherapyComment(String lesionAppropriateForCryotherapyComment) {
		this.lesionAppropriateForCryotherapyComment = lesionAppropriateForCryotherapyComment;
	}

	public Integer getMenarche() {
		return menarche;
	}

	public void setMenarche(Integer menarche) {
		this.menarche = menarche;
	}

	public Boolean getRegularMenses() {
		return regularMenses;
	}

	public void setRegularMenses(Boolean regularMenses) {
		this.regularMenses = regularMenses;
	}

	public String getMensesDaysApart() {
		return mensesDaysApart;
	}

	public void setMensesDaysApart(String mensesDaysApart) {
		this.mensesDaysApart = mensesDaysApart;
	}

	public Integer getMensesDaysDuration() {
		return mensesDaysDuration;
	}

	public void setMensesDaysDuration(Integer mensesDaysDuration) {
		this.mensesDaysDuration = mensesDaysDuration;
	}

	public String getNameOfProviderWhoPerformedVia() {
		return nameOfProviderWhoPerformedVia;
	}

	public void setNameOfProviderWhoPerformedVia(String nameOfProviderWhoPerformedVia) {
		this.nameOfProviderWhoPerformedVia = nameOfProviderWhoPerformedVia;
	}

	public String getNameOfProviderWhoReviewedCytology() {
		return nameOfProviderWhoReviewedCytology;
	}

	public void setNameOfProviderWhoReviewedCytology(String nameOfProviderWhoReviewedCytology) {
		this.nameOfProviderWhoReviewedCytology = nameOfProviderWhoReviewedCytology;
	}

	public Integer getNextCytologyVisitWeeks() {
		return nextCytologyVisitWeeks;
	}

	public void setNextCytologyVisitWeeks(Integer nextCytologyVisitWeeks) {
		this.nextCytologyVisitWeeks = nextCytologyVisitWeeks;
	}

	public Integer getNextVisitWeeks() {
		return nextVisitWeeks;
	}

	public void setNextVisitWeeks(Integer nextVisitWeeks) {
		this.nextVisitWeeks = nextVisitWeeks;
	}

	public Boolean getPapTestToday() {
		return papTestToday;
	}

	public void setPapTestToday(Boolean papTestToday) {
		this.papTestToday = papTestToday;
	}

	public Boolean getPostCoitalBleeding() {
		return postCoitalBleeding;
	}

	public void setPostCoitalBleeding(Boolean postCoitalBleeding) {
		this.postCoitalBleeding = postCoitalBleeding;
	}

	public Boolean getReferredForColposcopy() {
		return referredForColposcopy;
	}

	public void setReferredForColposcopy(Boolean referredForColposcopy) {
		this.referredForColposcopy = referredForColposcopy;
	}

	public Boolean getReferredForEvaluationWithGyn() {
		return referredForEvaluationWithGyn;
	}

	public void setReferredForEvaluationWithGyn(Boolean referredForEvaluationWithGyn) {
		this.referredForEvaluationWithGyn = referredForEvaluationWithGyn;
	}

	public Boolean getSelfBreastExamTaught() {
		return selfBreastExamTaught;
	}

	public void setSelfBreastExamTaught(Boolean selfBreastExamTaught) {
		this.selfBreastExamTaught = selfBreastExamTaught;
	}

	public Boolean getVaginalDischarge() {
		return vaginalDischarge;
	}

	public void setVaginalDischarge(Boolean vaginalDischarge) {
		this.vaginalDischarge = vaginalDischarge;
	}

	public Boolean getVaginalItchingBurning() {
		return vaginalItchingBurning;
	}

	public void setVaginalItchingBurning(Boolean vaginalItchingBurning) {
		this.vaginalItchingBurning = vaginalItchingBurning;
	}

	public String getViaCervixResult() {
		return viaCervixResult;
	}

	public void setViaCervixResult(String viaCervixResult) {
		this.viaCervixResult = viaCervixResult;
	}

	public SimpleLabResult getViaResults() {
		return viaResults;
	}

	public void setViaResults(SimpleLabResult viaResults) {
		this.viaResults = viaResults;
	}
}
