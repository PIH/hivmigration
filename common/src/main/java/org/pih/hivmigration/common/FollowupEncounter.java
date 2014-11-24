package org.pih.hivmigration.common;

import org.pih.hivmigration.common.code.ArtTreatmentRegimen;
import org.pih.hivmigration.common.code.DiarrheaDuration;
import org.pih.hivmigration.common.code.Location;
import org.pih.hivmigration.common.code.Severity;
import org.pih.hivmigration.common.code.TbTreatmentChangeReason;
import org.pih.hivmigration.common.code.TbTreatmentRegimen;
import org.pih.hivmigration.common.code.TreatmentStoppedReason;
import org.pih.hivmigration.common.util.ObsName;

import java.util.Date;

public class FollowupEncounter extends ClinicalEncounter {

	private String progress;
	private Boolean wellFollowed;
	private Boolean medToxicity;
	private String medToxicityComments;

	// Visit info
	@ObsName("routine_visit_p") Boolean routineVisit;
	@ObsName("patient_on_time_p") Boolean patientOnTime;
	@ObsName("patient_days_late") String patientDaysLate;  // TODO: messy contents of this might need to be cleaned up or discarded depending on what we want to do with it

	// Hospitalization since last visit
	// TODO: These presumably go with the hospitalized_since_last_visit question on the clinical encounter.  That should probably be moved here but need to investigate why there are intake obs
	@ObsName("hospitalized_diagnosis") String hospitalizationDiagnosis;
	@ObsName("hospitalized_duration") String hospitalizationDuration;

	// Accompnaniment and adherence
	@ObsName("visted_by_accomp_daily_p") Boolean accompagnateurVisitedDaily;  // TODO: Looks like a duplicate of clinical encounter: accompagnateurSeenEachDay.  Review this.
	@ObsName("accomp_changed_since_last_visit") Boolean accompagnateurChangedSinceLastVisit;
	@ObsName("accomp_change_date") Date accompagnateurChangeDate;
	@ObsName("accomp_change_reason") String accompagnateurChangeReason;
	@ObsName("accompagnateur_missed_visits_reason") String reasonAccompagnateurMissedVisits;
	@ObsName("adherence_why_missed") String adherenceMissedReason;
	@ObsName("patient_does_not_receive_art_p") Boolean adherencePatientDoesNotReceiveArt;
	@ObsName("num_doses_missed_last_month") String numDosesMissedLastMonth; // TODO: Data here is texty and unclear how this relates to num missed in last 4 days question on clinical encounter

	// Socio-economic complaints / problems
	@ObsName("complaint_clean_water") Boolean complaintCleanWater;
	@ObsName("complaint_funeral_expenses") Boolean complaintFuneralExpenses;
	@ObsName("complaint_housing_expenses") Boolean complaintHousingExpenses;
	@ObsName("complaint_hunger") Boolean complaintHungry;
	@ObsName("complaint_school_expenses") Boolean complaintSchoolExpenses;
	@ObsName("complaint_unemployment") Boolean complaintUnemployment;
	@ObsName("complaint_other_p") Boolean complaintOther;
	@ObsName("complaint_other") String complaintOtherDescription;

	// Current Tx
	@ObsName("current_tx.art") ArtTreatmentRegimen currentArtTreatment;
	@ObsName("current_tx.art_other") String currentArtTreatmentOther;
	@ObsName("current_tx.art_start_date") Date currentArtTreatmentStartDate;
	@ObsName("current_tx.prophylaxis_CTX") Boolean currentlyOnCtxProphylaxis;
	@ObsName("current_tx.prophylaxis_CTX_start_date") Date currentCtxProphylaxisStartDate;
	@ObsName("current_tx.prophylaxis_Fluconazole") Boolean currentlyOnFluconazoleProphylaxis;
	@ObsName("current_tx.prophylaxis_Fluconazole_start_date") Date currentlyFluconazoleProphylaxisStartDate;
	@ObsName("current_tx.prophylaxis_Isoniazid") Boolean currentlyOnInhProphylaxis;
	@ObsName("current_tx.prophylaxis_Isoniazid_start_date") Date currentInhProphylaxisStartDate;
	@ObsName("current_tx.tb") TbTreatmentRegimen currentTbTreatment;
	@ObsName("current_tx.tb_other") String currentTbTreatmentOther;
	@ObsName("current_tx.tb_start_date") Date currentTbTreatmentStartDate;
	@ObsName("current_tx.other_medications") String currentOtherMedications;
	@ObsName("other_prophylactic_changes") String otherProphylacticChanges;  // TODO: Values for this obs are very sparse and data looks very bad quality.  Remove?

	// TODO: These Obs are related to the Symptom of diarrhea being indicated, to provide additional details
	@ObsName("diarrhea_duration") DiarrheaDuration diarrheaDuration;
	@ObsName("diarrhea_frequency_per_day") String diarrheaFrequencyPerDay;

	// Family Planning Current / History
	@ObsName("sexually_active_p") Boolean sexuallyActive;
	@ObsName("family_planning_used") Boolean familyPlanningUsed;
	@ObsName("family_planning_why_not") String familyPlanningNotUsedReason;
	@ObsName("family_planning.abstinence=t|family_planning_method=abstinence") Boolean familyPlanningAbstinence;
	@ObsName("family_planning.condom=t|family_planning_method=condoms") Boolean familyPlanningCondom;
	@ObsName("family_planning.norplant=t|family_planning_method=Norplant") Boolean familyPlanningNorplant;
	@ObsName("family_planning.oral_contraceptive=t|family_planning_method=oral_contraceptives") Boolean familyPlanningOralContraceptive;
	@ObsName("family_planning_method=ligation|family_planning_method=ligature") Boolean familyPlanningLigation;
	@ObsName("family_planning_method=vasectomy") Boolean familyPlanningVasectomy;
	@ObsName("family_planning_method=DMPA") Boolean familyPlanningDmpa; // TODO: Figure out if this is a duplicate
	@ObsName("family_planning_method=Lofemenal") Boolean familyPlanningLofemenal; // TODO: Figure out if this is a duplicate
	@ObsName("family_planning_method=other") Boolean familyPlanningOther;
	@ObsName("family_planning_method_other|family_planning_other") String familyPlanningOtherDescription;

	// Family Planning Recommendations
	@ObsName("family_planning_recommended") Boolean familyPlanningRecommended;
	@ObsName("family_planning_recommendation") String familyPlanningRecommendation;
	@ObsName("family_planning_recommendation_accepted") Boolean familyPlanningRecommendationAccepted;
	@ObsName("family_planning_recommendation_refused_reason") String familyPlanningRecommendationRefusedReason;

	// Nutritional Assistance
	@ObsName("receiving_nutritional_assistance_p") Boolean receivingNutritionalAssistance;
	@ObsName("receiving_nutritional_assistance_dry_ration") Boolean receivingNutritionalAssistanceDryRation;
	@ObsName("receiving_nutritional_assistance_financial_aid") Boolean receivingNutritionalAssistanceFinancialAid;
	@ObsName("receiving_nutritional_assistance_reason") String receivingNutritionalAssistanceReason; // TODO: Only 3 obs for this, one of which looks like a test at least.  Remove?
	@ObsName("last_nutritional_assistance_date") Date lastNutritionalAssistanceDate;

	// Additional information related to tb treatment orders
	@ObsName("tb_treatment_reason") TbTreatmentChangeReason tbTreatmentChangeReason;
	@ObsName("tb_treatment_reason_other") String tbTreatmentChangeReasonOther;

	// After plan, before next appointment date, text input asking "Bref résumé du statut du patient"
	@ObsName("patient_status_summary") String statusSummary;

	@ObsName("transfer_out_to") Location transferOutTo; // TODO: It appears as though this is asked on the intake form too.  Maybe no values though?  Consider moving to clinical encounter.

	@ObsName("treatment_stopped_reason") TreatmentStoppedReason treatmentStoppedReason;
	@ObsName("treatment_stopped_reason_side_effect") String treatmentStoppedReasonSideEffect; // TODO: looks like barely any data, and really bad data.  Something went wrong.  Remove?

	@ObsName("side_effect.none") Boolean noSideEffects;
	@ObsName("side_effect.anemia=t|side_effect=anemia") Boolean sideEffectAnemia;
	@ObsName("side_effect.neuropathy=t|side_effect=neuropathy") Boolean sideEffectNeuropathy;
	@ObsName("side_effect.hepatitis=t|side_effect=hepatitis") Boolean sideEffectHepatitis;
	@ObsName("side_effect.nausea_vomiting=t|side_effect=nausea") Boolean sideEffectNauseaVomiting;
	@ObsName("side_effect.icterus=t|side_effect=jaundice") Boolean sideEffectJaundice;
	@ObsName("side_effect.lactic_acidosis=t|side_effect=lactic_acid") Boolean sideEffectLacticAcidosis;
	@ObsName("side_effect=cns_symptoms") Boolean sideEffectCnsSymptoms;
	@ObsName("side_effect=diarrhea") Boolean sideEffectDiarrhea;

	// TODO: We collect rash+severity in two different ways.  Might just be easiest to migrate them into the same concepts at the import end.
	@ObsName("side_effect=rash") Boolean sideEffectRash;
	@ObsName("side_effect_rash_severity") Severity sideEffectRashSeverity;
	@ObsName("side_effect.mild_rash") Boolean sideEffectMildRash;
	@ObsName("side_effect.moderate_rash") Boolean sideEffectModerateRash;
	@ObsName("side_effect.severe_rash") Boolean sideEffectSevereRash;

	@ObsName("side_effect.other=t|side_effect=other") Boolean sideEffectOther;
	@ObsName("side_effect_other") String sideEffectOtherDetails;

	public FollowupEncounter() {}

	public String getProgress() {
		return progress;
	}

	public void setProgress(String progress) {
		this.progress = progress;
	}

	public Boolean getWellFollowed() {
		return wellFollowed;
	}

	public void setWellFollowed(Boolean wellFollowed) {
		this.wellFollowed = wellFollowed;
	}

	public Boolean getMedToxicity() {
		return medToxicity;
	}

	public void setMedToxicity(Boolean medToxicity) {
		this.medToxicity = medToxicity;
	}

	public String getMedToxicityComments() {
		return medToxicityComments;
	}

	public void setMedToxicityComments(String medToxicityComments) {
		this.medToxicityComments = medToxicityComments;
	}

	public Boolean getRoutineVisit() {
		return routineVisit;
	}

	public void setRoutineVisit(Boolean routineVisit) {
		this.routineVisit = routineVisit;
	}

	public Boolean getPatientOnTime() {
		return patientOnTime;
	}

	public void setPatientOnTime(Boolean patientOnTime) {
		this.patientOnTime = patientOnTime;
	}

	public String getPatientDaysLate() {
		return patientDaysLate;
	}

	public void setPatientDaysLate(String patientDaysLate) {
		this.patientDaysLate = patientDaysLate;
	}

	public String getHospitalizationDiagnosis() {
		return hospitalizationDiagnosis;
	}

	public void setHospitalizationDiagnosis(String hospitalizationDiagnosis) {
		this.hospitalizationDiagnosis = hospitalizationDiagnosis;
	}

	public String getHospitalizationDuration() {
		return hospitalizationDuration;
	}

	public void setHospitalizationDuration(String hospitalizationDuration) {
		this.hospitalizationDuration = hospitalizationDuration;
	}

	public Boolean getAccompagnateurVisitedDaily() {
		return accompagnateurVisitedDaily;
	}

	public void setAccompagnateurVisitedDaily(Boolean accompagnateurVisitedDaily) {
		this.accompagnateurVisitedDaily = accompagnateurVisitedDaily;
	}

	public Boolean getAccompagnateurChangedSinceLastVisit() {
		return accompagnateurChangedSinceLastVisit;
	}

	public void setAccompagnateurChangedSinceLastVisit(Boolean accompagnateurChangedSinceLastVisit) {
		this.accompagnateurChangedSinceLastVisit = accompagnateurChangedSinceLastVisit;
	}

	public Date getAccompagnateurChangeDate() {
		return accompagnateurChangeDate;
	}

	public void setAccompagnateurChangeDate(Date accompagnateurChangeDate) {
		this.accompagnateurChangeDate = accompagnateurChangeDate;
	}

	public String getAccompagnateurChangeReason() {
		return accompagnateurChangeReason;
	}

	public void setAccompagnateurChangeReason(String accompagnateurChangeReason) {
		this.accompagnateurChangeReason = accompagnateurChangeReason;
	}

	public String getReasonAccompagnateurMissedVisits() {
		return reasonAccompagnateurMissedVisits;
	}

	public void setReasonAccompagnateurMissedVisits(String reasonAccompagnateurMissedVisits) {
		this.reasonAccompagnateurMissedVisits = reasonAccompagnateurMissedVisits;
	}

	public String getAdherenceMissedReason() {
		return adherenceMissedReason;
	}

	public void setAdherenceMissedReason(String adherenceMissedReason) {
		this.adherenceMissedReason = adherenceMissedReason;
	}

	public Boolean getAdherencePatientDoesNotReceiveArt() {
		return adherencePatientDoesNotReceiveArt;
	}

	public void setAdherencePatientDoesNotReceiveArt(Boolean adherencePatientDoesNotReceiveArt) {
		this.adherencePatientDoesNotReceiveArt = adherencePatientDoesNotReceiveArt;
	}

	public String getNumDosesMissedLastMonth() {
		return numDosesMissedLastMonth;
	}

	public void setNumDosesMissedLastMonth(String numDosesMissedLastMonth) {
		this.numDosesMissedLastMonth = numDosesMissedLastMonth;
	}

	public Boolean getComplaintCleanWater() {
		return complaintCleanWater;
	}

	public void setComplaintCleanWater(Boolean complaintCleanWater) {
		this.complaintCleanWater = complaintCleanWater;
	}

	public Boolean getComplaintFuneralExpenses() {
		return complaintFuneralExpenses;
	}

	public void setComplaintFuneralExpenses(Boolean complaintFuneralExpenses) {
		this.complaintFuneralExpenses = complaintFuneralExpenses;
	}

	public Boolean getComplaintHousingExpenses() {
		return complaintHousingExpenses;
	}

	public void setComplaintHousingExpenses(Boolean complaintHousingExpenses) {
		this.complaintHousingExpenses = complaintHousingExpenses;
	}

	public Boolean getComplaintHungry() {
		return complaintHungry;
	}

	public void setComplaintHungry(Boolean complaintHungry) {
		this.complaintHungry = complaintHungry;
	}

	public Boolean getComplaintSchoolExpenses() {
		return complaintSchoolExpenses;
	}

	public void setComplaintSchoolExpenses(Boolean complaintSchoolExpenses) {
		this.complaintSchoolExpenses = complaintSchoolExpenses;
	}

	public Boolean getComplaintUnemployment() {
		return complaintUnemployment;
	}

	public void setComplaintUnemployment(Boolean complaintUnemployment) {
		this.complaintUnemployment = complaintUnemployment;
	}

	public Boolean getComplaintOther() {
		return complaintOther;
	}

	public void setComplaintOther(Boolean complaintOther) {
		this.complaintOther = complaintOther;
	}

	public String getComplaintOtherDescription() {
		return complaintOtherDescription;
	}

	public void setComplaintOtherDescription(String complaintOtherDescription) {
		this.complaintOtherDescription = complaintOtherDescription;
	}

	public ArtTreatmentRegimen getCurrentArtTreatment() {
		return currentArtTreatment;
	}

	public void setCurrentArtTreatment(ArtTreatmentRegimen currentArtTreatment) {
		this.currentArtTreatment = currentArtTreatment;
	}

	public String getCurrentArtTreatmentOther() {
		return currentArtTreatmentOther;
	}

	public void setCurrentArtTreatmentOther(String currentArtTreatmentOther) {
		this.currentArtTreatmentOther = currentArtTreatmentOther;
	}

	public Date getCurrentArtTreatmentStartDate() {
		return currentArtTreatmentStartDate;
	}

	public void setCurrentArtTreatmentStartDate(Date currentArtTreatmentStartDate) {
		this.currentArtTreatmentStartDate = currentArtTreatmentStartDate;
	}

	public Boolean getCurrentlyOnCtxProphylaxis() {
		return currentlyOnCtxProphylaxis;
	}

	public void setCurrentlyOnCtxProphylaxis(Boolean currentlyOnCtxProphylaxis) {
		this.currentlyOnCtxProphylaxis = currentlyOnCtxProphylaxis;
	}

	public Date getCurrentCtxProphylaxisStartDate() {
		return currentCtxProphylaxisStartDate;
	}

	public void setCurrentCtxProphylaxisStartDate(Date currentCtxProphylaxisStartDate) {
		this.currentCtxProphylaxisStartDate = currentCtxProphylaxisStartDate;
	}

	public Boolean getCurrentlyOnFluconazoleProphylaxis() {
		return currentlyOnFluconazoleProphylaxis;
	}

	public void setCurrentlyOnFluconazoleProphylaxis(Boolean currentlyOnFluconazoleProphylaxis) {
		this.currentlyOnFluconazoleProphylaxis = currentlyOnFluconazoleProphylaxis;
	}

	public Date getCurrentlyFluconazoleProphylaxisStartDate() {
		return currentlyFluconazoleProphylaxisStartDate;
	}

	public void setCurrentlyFluconazoleProphylaxisStartDate(Date currentlyFluconazoleProphylaxisStartDate) {
		this.currentlyFluconazoleProphylaxisStartDate = currentlyFluconazoleProphylaxisStartDate;
	}

	public Boolean getCurrentlyOnInhProphylaxis() {
		return currentlyOnInhProphylaxis;
	}

	public void setCurrentlyOnInhProphylaxis(Boolean currentlyOnInhProphylaxis) {
		this.currentlyOnInhProphylaxis = currentlyOnInhProphylaxis;
	}

	public Date getCurrentInhProphylaxisStartDate() {
		return currentInhProphylaxisStartDate;
	}

	public void setCurrentInhProphylaxisStartDate(Date currentInhProphylaxisStartDate) {
		this.currentInhProphylaxisStartDate = currentInhProphylaxisStartDate;
	}

	public TbTreatmentRegimen getCurrentTbTreatment() {
		return currentTbTreatment;
	}

	public void setCurrentTbTreatment(TbTreatmentRegimen currentTbTreatment) {
		this.currentTbTreatment = currentTbTreatment;
	}

	public String getCurrentTbTreatmentOther() {
		return currentTbTreatmentOther;
	}

	public void setCurrentTbTreatmentOther(String currentTbTreatmentOther) {
		this.currentTbTreatmentOther = currentTbTreatmentOther;
	}

	public Date getCurrentTbTreatmentStartDate() {
		return currentTbTreatmentStartDate;
	}

	public void setCurrentTbTreatmentStartDate(Date currentTbTreatmentStartDate) {
		this.currentTbTreatmentStartDate = currentTbTreatmentStartDate;
	}

	public String getCurrentOtherMedications() {
		return currentOtherMedications;
	}

	public void setCurrentOtherMedications(String currentOtherMedications) {
		this.currentOtherMedications = currentOtherMedications;
	}

	public String getOtherProphylacticChanges() {
		return otherProphylacticChanges;
	}

	public void setOtherProphylacticChanges(String otherProphylacticChanges) {
		this.otherProphylacticChanges = otherProphylacticChanges;
	}

	public DiarrheaDuration getDiarrheaDuration() {
		return diarrheaDuration;
	}

	public void setDiarrheaDuration(DiarrheaDuration diarrheaDuration) {
		this.diarrheaDuration = diarrheaDuration;
	}

	public String getDiarrheaFrequencyPerDay() {
		return diarrheaFrequencyPerDay;
	}

	public void setDiarrheaFrequencyPerDay(String diarrheaFrequencyPerDay) {
		this.diarrheaFrequencyPerDay = diarrheaFrequencyPerDay;
	}

	public Boolean getSexuallyActive() {
		return sexuallyActive;
	}

	public void setSexuallyActive(Boolean sexuallyActive) {
		this.sexuallyActive = sexuallyActive;
	}

	public Boolean getFamilyPlanningUsed() {
		return familyPlanningUsed;
	}

	public void setFamilyPlanningUsed(Boolean familyPlanningUsed) {
		this.familyPlanningUsed = familyPlanningUsed;
	}

	public String getFamilyPlanningNotUsedReason() {
		return familyPlanningNotUsedReason;
	}

	public void setFamilyPlanningNotUsedReason(String familyPlanningNotUsedReason) {
		this.familyPlanningNotUsedReason = familyPlanningNotUsedReason;
	}

	public Boolean getFamilyPlanningAbstinence() {
		return familyPlanningAbstinence;
	}

	public void setFamilyPlanningAbstinence(Boolean familyPlanningAbstinence) {
		this.familyPlanningAbstinence = familyPlanningAbstinence;
	}

	public Boolean getFamilyPlanningCondom() {
		return familyPlanningCondom;
	}

	public void setFamilyPlanningCondom(Boolean familyPlanningCondom) {
		this.familyPlanningCondom = familyPlanningCondom;
	}

	public Boolean getFamilyPlanningNorplant() {
		return familyPlanningNorplant;
	}

	public void setFamilyPlanningNorplant(Boolean familyPlanningNorplant) {
		this.familyPlanningNorplant = familyPlanningNorplant;
	}

	public Boolean getFamilyPlanningOralContraceptive() {
		return familyPlanningOralContraceptive;
	}

	public void setFamilyPlanningOralContraceptive(Boolean familyPlanningOralContraceptive) {
		this.familyPlanningOralContraceptive = familyPlanningOralContraceptive;
	}

	public Boolean getFamilyPlanningLigation() {
		return familyPlanningLigation;
	}

	public void setFamilyPlanningLigation(Boolean familyPlanningLigation) {
		this.familyPlanningLigation = familyPlanningLigation;
	}

	public Boolean getFamilyPlanningVasectomy() {
		return familyPlanningVasectomy;
	}

	public void setFamilyPlanningVasectomy(Boolean familyPlanningVasectomy) {
		this.familyPlanningVasectomy = familyPlanningVasectomy;
	}

	public Boolean getFamilyPlanningDmpa() {
		return familyPlanningDmpa;
	}

	public void setFamilyPlanningDmpa(Boolean familyPlanningDmpa) {
		this.familyPlanningDmpa = familyPlanningDmpa;
	}

	public Boolean getFamilyPlanningLofemenal() {
		return familyPlanningLofemenal;
	}

	public void setFamilyPlanningLofemenal(Boolean familyPlanningLofemenal) {
		this.familyPlanningLofemenal = familyPlanningLofemenal;
	}

	public Boolean getFamilyPlanningOther() {
		return familyPlanningOther;
	}

	public void setFamilyPlanningOther(Boolean familyPlanningOther) {
		this.familyPlanningOther = familyPlanningOther;
	}

	public String getFamilyPlanningOtherDescription() {
		return familyPlanningOtherDescription;
	}

	public void setFamilyPlanningOtherDescription(String familyPlanningOtherDescription) {
		this.familyPlanningOtherDescription = familyPlanningOtherDescription;
	}

	public Boolean getFamilyPlanningRecommended() {
		return familyPlanningRecommended;
	}

	public void setFamilyPlanningRecommended(Boolean familyPlanningRecommended) {
		this.familyPlanningRecommended = familyPlanningRecommended;
	}

	public String getFamilyPlanningRecommendation() {
		return familyPlanningRecommendation;
	}

	public void setFamilyPlanningRecommendation(String familyPlanningRecommendation) {
		this.familyPlanningRecommendation = familyPlanningRecommendation;
	}

	public Boolean getFamilyPlanningRecommendationAccepted() {
		return familyPlanningRecommendationAccepted;
	}

	public void setFamilyPlanningRecommendationAccepted(Boolean familyPlanningRecommendationAccepted) {
		this.familyPlanningRecommendationAccepted = familyPlanningRecommendationAccepted;
	}

	public String getFamilyPlanningRecommendationRefusedReason() {
		return familyPlanningRecommendationRefusedReason;
	}

	public void setFamilyPlanningRecommendationRefusedReason(String familyPlanningRecommendationRefusedReason) {
		this.familyPlanningRecommendationRefusedReason = familyPlanningRecommendationRefusedReason;
	}

	public Boolean getReceivingNutritionalAssistance() {
		return receivingNutritionalAssistance;
	}

	public void setReceivingNutritionalAssistance(Boolean receivingNutritionalAssistance) {
		this.receivingNutritionalAssistance = receivingNutritionalAssistance;
	}

	public Boolean getReceivingNutritionalAssistanceDryRation() {
		return receivingNutritionalAssistanceDryRation;
	}

	public void setReceivingNutritionalAssistanceDryRation(Boolean receivingNutritionalAssistanceDryRation) {
		this.receivingNutritionalAssistanceDryRation = receivingNutritionalAssistanceDryRation;
	}

	public Boolean getReceivingNutritionalAssistanceFinancialAid() {
		return receivingNutritionalAssistanceFinancialAid;
	}

	public void setReceivingNutritionalAssistanceFinancialAid(Boolean receivingNutritionalAssistanceFinancialAid) {
		this.receivingNutritionalAssistanceFinancialAid = receivingNutritionalAssistanceFinancialAid;
	}

	public String getReceivingNutritionalAssistanceReason() {
		return receivingNutritionalAssistanceReason;
	}

	public void setReceivingNutritionalAssistanceReason(String receivingNutritionalAssistanceReason) {
		this.receivingNutritionalAssistanceReason = receivingNutritionalAssistanceReason;
	}

	public Date getLastNutritionalAssistanceDate() {
		return lastNutritionalAssistanceDate;
	}

	public void setLastNutritionalAssistanceDate(Date lastNutritionalAssistanceDate) {
		this.lastNutritionalAssistanceDate = lastNutritionalAssistanceDate;
	}

	public TbTreatmentChangeReason getTbTreatmentChangeReason() {
		return tbTreatmentChangeReason;
	}

	public void setTbTreatmentChangeReason(TbTreatmentChangeReason tbTreatmentChangeReason) {
		this.tbTreatmentChangeReason = tbTreatmentChangeReason;
	}

	public String getTbTreatmentChangeReasonOther() {
		return tbTreatmentChangeReasonOther;
	}

	public void setTbTreatmentChangeReasonOther(String tbTreatmentChangeReasonOther) {
		this.tbTreatmentChangeReasonOther = tbTreatmentChangeReasonOther;
	}

	public String getStatusSummary() {
		return statusSummary;
	}

	public void setStatusSummary(String statusSummary) {
		this.statusSummary = statusSummary;
	}

	public Location getTransferOutTo() {
		return transferOutTo;
	}

	public void setTransferOutTo(Location transferOutTo) {
		this.transferOutTo = transferOutTo;
	}

	public TreatmentStoppedReason getTreatmentStoppedReason() {
		return treatmentStoppedReason;
	}

	public void setTreatmentStoppedReason(TreatmentStoppedReason treatmentStoppedReason) {
		this.treatmentStoppedReason = treatmentStoppedReason;
	}

	public String getTreatmentStoppedReasonSideEffect() {
		return treatmentStoppedReasonSideEffect;
	}

	public void setTreatmentStoppedReasonSideEffect(String treatmentStoppedReasonSideEffect) {
		this.treatmentStoppedReasonSideEffect = treatmentStoppedReasonSideEffect;
	}

	public Boolean getNoSideEffects() {
		return noSideEffects;
	}

	public void setNoSideEffects(Boolean noSideEffects) {
		this.noSideEffects = noSideEffects;
	}

	public Boolean getSideEffectAnemia() {
		return sideEffectAnemia;
	}

	public void setSideEffectAnemia(Boolean sideEffectAnemia) {
		this.sideEffectAnemia = sideEffectAnemia;
	}

	public Boolean getSideEffectNeuropathy() {
		return sideEffectNeuropathy;
	}

	public void setSideEffectNeuropathy(Boolean sideEffectNeuropathy) {
		this.sideEffectNeuropathy = sideEffectNeuropathy;
	}

	public Boolean getSideEffectHepatitis() {
		return sideEffectHepatitis;
	}

	public void setSideEffectHepatitis(Boolean sideEffectHepatitis) {
		this.sideEffectHepatitis = sideEffectHepatitis;
	}

	public Boolean getSideEffectNauseaVomiting() {
		return sideEffectNauseaVomiting;
	}

	public void setSideEffectNauseaVomiting(Boolean sideEffectNauseaVomiting) {
		this.sideEffectNauseaVomiting = sideEffectNauseaVomiting;
	}

	public Boolean getSideEffectJaundice() {
		return sideEffectJaundice;
	}

	public void setSideEffectJaundice(Boolean sideEffectJaundice) {
		this.sideEffectJaundice = sideEffectJaundice;
	}

	public Boolean getSideEffectLacticAcidosis() {
		return sideEffectLacticAcidosis;
	}

	public void setSideEffectLacticAcidosis(Boolean sideEffectLacticAcidosis) {
		this.sideEffectLacticAcidosis = sideEffectLacticAcidosis;
	}

	public Boolean getSideEffectCnsSymptoms() {
		return sideEffectCnsSymptoms;
	}

	public void setSideEffectCnsSymptoms(Boolean sideEffectCnsSymptoms) {
		this.sideEffectCnsSymptoms = sideEffectCnsSymptoms;
	}

	public Boolean getSideEffectDiarrhea() {
		return sideEffectDiarrhea;
	}

	public void setSideEffectDiarrhea(Boolean sideEffectDiarrhea) {
		this.sideEffectDiarrhea = sideEffectDiarrhea;
	}

	public Boolean getSideEffectRash() {
		return sideEffectRash;
	}

	public void setSideEffectRash(Boolean sideEffectRash) {
		this.sideEffectRash = sideEffectRash;
	}

	public Severity getSideEffectRashSeverity() {
		return sideEffectRashSeverity;
	}

	public void setSideEffectRashSeverity(Severity sideEffectRashSeverity) {
		this.sideEffectRashSeverity = sideEffectRashSeverity;
	}

	public Boolean getSideEffectMildRash() {
		return sideEffectMildRash;
	}

	public void setSideEffectMildRash(Boolean sideEffectMildRash) {
		this.sideEffectMildRash = sideEffectMildRash;
	}

	public Boolean getSideEffectModerateRash() {
		return sideEffectModerateRash;
	}

	public void setSideEffectModerateRash(Boolean sideEffectModerateRash) {
		this.sideEffectModerateRash = sideEffectModerateRash;
	}

	public Boolean getSideEffectSevereRash() {
		return sideEffectSevereRash;
	}

	public void setSideEffectSevereRash(Boolean sideEffectSevereRash) {
		this.sideEffectSevereRash = sideEffectSevereRash;
	}

	public Boolean getSideEffectOther() {
		return sideEffectOther;
	}

	public void setSideEffectOther(Boolean sideEffectOther) {
		this.sideEffectOther = sideEffectOther;
	}

	public String getSideEffectOtherDetails() {
		return sideEffectOtherDetails;
	}

	public void setSideEffectOtherDetails(String sideEffectOtherDetails) {
		this.sideEffectOtherDetails = sideEffectOtherDetails;
	}
}
