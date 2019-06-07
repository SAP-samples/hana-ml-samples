/*********************************************************************************************************************************/
/*** SAP HANA Predictive and Machine Learning - Predictive Analysis Library samples **********************************************/
/*** The sample code solemnly targets personal educational purposes **************************************************************/
/*** RandomDecisionTrees sample code, based on SAP HANA 2 SPS02 and above ********************************************************/
/*** Version 1, created 7th June 2019 ********************************************************************************************/
/*********************************************************************************************************************************/

/*********************************************************************************************************************************/
/*** User schema: the sample refers to sample data to be stored in the database schema HANAML_SAMPLES ****************************/
/*********************************************************************************************************************************/

/*********************************************************************************************************************************/
/*** Scenario: Train a RandomDecisionTree model to predict who would be interested in buying a caravan insurance policy **********/
/*** The use case is based on: P. van der Putten and M. van Someren (eds) . CoIL Challenge 2000: The Insurance Company Case. *****/
/****************************  Published by Sentient Machine Research, Amsterdam. Also a Leiden Institute of Advanced Computer ***/
/****************************  Science Technical Report 2000-09. June 22, 2000. See http://www.liacs.nl/~putten/library/cc2000/ **/
/*** The referenced datasets have to downloaded and loaded into the tables defined in the appendix *******************************/
/*** The objective is to predict as many true positives on the test set as possible **********************************************/
/*********************************************************************************************************************************/

/*** The sample code is based on SQLScript anonymous block, including the following steps: ***************************************/
-- 1. Declaring variables;
-- 2. Defining the input train and test data;
-- 3. Setting the algorithm parameters and training the RandomDecisionTree model ;
-- 4. Evaluating the model against the test data using AUC, ConfusionMatrix and its statistics;
-- Appendix: Table definitions;
/*********************************************************************************************************************************/

DO 
 begin
    /*** STEP1 - Declaration of Variables **************************************/    
    DECLARE lt_PAL_PARAMETER_TBL TABLE("PARAM_NAME" VARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100));
    DECLARE lt_PAL_RDT_MODEL_TBL TABLE("ROW_INDEX" INTEGER,	"TREE_INDEX" INTEGER,	"MODEL_CONTENT" NVARCHAR(5000));
    DECLARE lt_P4 /*VARIABLE_IMPORTANCE*/ TABLE ("VARIABLE_NAME" NVARCHAR(48),	"IMPORTANCE" DOUBLE);
    DECLARE lt_P5 /*OUT_OF_BAG_ERROR*/ TABLE ("TREE_INDEX" INTEGER,	"ERROR" DOUBLE );
    DECLARE lt_P6 /*CONFUSION_MATRIX*/ TABLE ("ACTUAL_CLASS" NVARCHAR(100),	"PREDICTED_CLASS" NVARCHAR(100),	"COUNT" Double );
    DECLARE lt_PAL_PREDICT_RESULT TABLE ("ID" INTEGER,	"SCORE" NVARCHAR(100),	"CONFIDENCE" Double );
    DECLARE lt_PAL_AUC_OUT_ROCdata TABLE ("ID" INTEGER, "FPR" DOUBLE, "TPR" DOUBLE);
    DECLARE lt_PAL_AUC_OUT_AUCstats TABLE ( "STAT_NAME"  NVARCHAR(100), "STAT_VALUE" DOUBLE);
    DECLARE lt_PAL_CF_MATRIX /*CONFUSION_Matrix */ TABLE ("ORIGINAL_LABEL" NVARCHAR(100),	"PREDICTED_LABEL" NVARCHAR(100),	"COUNT" Double );
    DECLARE lt_PAL_CF_CLASSREPORT /*CLASSIFICATION REPORT*/ TABLE (CLASS NVARCHAR(100), RECALL DOUBLE, "PRECISION" DOUBLE, F_MEASURE DOUBLE, SUPPORT INTEGER);
       
	/*** STEP2 - Define INPUT DATA **********************************************/
	lt_traindata= select * from "HANAML_SAMPLES"."INSURANCE_TRAIN";
	lt_testdata= select * from "HANAML_SAMPLES"."INSURANCE_TEST";
	
	lt_testdata_PRED = SELECT "ID", "Customer_Subtype","Number_of_houses","Avg_size_household","Avg_age","Customer_main_type","Roman_catholic","Protestant","Other_religion","No_religion","Married","Living_together",
	"Other_relation","Singles","Household_without_children","Household_with_children","High_level_education","Medium_level_education","Lower_level_education","High_status","Entrepreneur",
	"Farmer","Middle_management","Skilled_labourers","Unskilled_labourers","Social_class_A","Social_class_B1","Social_class_B2","Social_class_C","Social_class_D","Rented_house","Home_owners",
	"One_car","Two_cars","No_car","National_Health_Service","Private_health_insurance","Income_LT_30000","Income_30_45000","Income_45_75000","Income_75_122000","Income_GT123000","Average_income",
	"Purchasing_power_class","Contribution_third_party_insurance_firms","Contribution_third_party_insurane_agriculture","Contribution_car_policies","Contribution_delivery_van_policies",
	"Contribution_motorcycle_scooter_policies","Contribution_lorry_policies","Contribution_trailer_policies","Contribution_tractor_policies","Contribution_agricultural_machines_policies",
	"Contribution_moped_policies","Contribution_life_insurances","Contribution_private_accident_insurance_policies","Contribution_family_accidents_insurance_policies",
	"Contribution_disability_insurance_policies","Contribution_fire_policies","Contribution_surfboard_policies","Contribution_boat_policies","Contribution_bicycle_policies",
	"Contribution_property_insurance_policies","Contribution_social_security_insurance_policies","Number_of_private_third_party_insurance","Number_of_third_party_insurance_firms",
	"Number_of_third_party_insurane_agriculture","Number_of_car_policies","Number_of_delivery_van_policies","Number_of_motorcycle_scooter_policies","Number_of_lorry_policies",
	"Number_of_trailer_policies","Number_of_tractor_policies","Number_of_agricultural_machines_policies","Number_of_moped_policies","Number_of_life_insurances",
	"Number_of_private_accident_insurance_policies","Number_of_family_accidents_insurance_policies","Number_of_disability_insurance_policies","Number_of_fire_policies",
	"Number_of_surfboard_policies","Number_of_boat_policies","Number_of_bicycle_policies","Number_of_property_insurance_policies","Number_of_social_security_insurance_policies" from :lt_testdata;
	--select * from :lt_traindata;
	--select * from :lt_traindata_PRED;

	/*** STEP3 - FIT MODEL ********************************************************/
	/* STEP3.1 Populating the Paramter Table Variable */  
	  :lt_PAL_PARAMETER_TBL.DELETE();
	  :lt_PAL_PARAMETER_TBL.INSERT(('HAS_ID',1,NULL,NULL),	1); --first column is the ID column;
 	  :lt_PAL_PARAMETER_TBL.INSERT(('DEPENDENT_VARIABLE',NULL,NULL,'Number_of_mobile_home_policies_num'),2); --Target or dependent variable;
      :lt_PAL_PARAMETER_TBL.INSERT(('ALLOW_MISSING_LABEL',NULL,0 ,NULL),3); --Target variable must not have missing values;
	  :lt_PAL_PARAMETER_TBL.INSERT(('SEED',1234, NULL,NULL),		4);	--Use this see for repeatability;
	  :lt_PAL_PARAMETER_TBL.INSERT(('THREAD_RATIO', NULL, 0.25 , NULL),5); --Use maximum of 25% of available threads;
	  
	  :lt_PAL_PARAMETER_TBL.INSERT(('TREES_NUM', 200, NULL,NULL),		6); --n.estimators/number of modeled decision trees;
      :lt_PAL_PARAMETER_TBL.INSERT(('TRY_NUM', 8,  NULL, NULL),		7); -- max_features/max number of randomly selected splitting variables;
      :lt_PAL_PARAMETER_TBL.INSERT(('NODE_SIZE', 1, NULL, NULL),8); -- min_samples_leaf/minimum number of records in a leaf;
	  :lt_PAL_PARAMETER_TBL.INSERT(('MAX_DEPTH', 55,    NULL, NULL),9); -- The maximum depth of each tree;
      :lt_PAL_PARAMETER_TBL.INSERT(('SPLIT_THRESHOLD', NULL, 0.0000001, NULL),10); -- If the improvement value of the best split is less than this value, the tree stops growing;
      
      :lt_PAL_PARAMETER_TBL.INSERT(('CALCULATE_OOB', 1, NULL, NULL),11); --Indicates whether to calculate out-of-bag error;
 
      --A possible setting for stratified sampling;
      :lt_PAL_PARAMETER_TBL.INSERT(('SAMPLE_FRACTION', NULL, 1 , NULL),12);--The fraction of data used for training;
      :lt_PAL_PARAMETER_TBL.INSERT(('STRATA', NULL, 0.05 , '0'),13); --The class label and its proportion that this class occupies in the sampling data.;
      :lt_PAL_PARAMETER_TBL.INSERT(('STRATA', NULL, 0.95 , '1'),14);
      
      -- A possible setting for prior probabilities for classification;
      :lt_PAL_PARAMETER_TBL.INSERT(('PRIORS', NULL, 0.05 , '1'),15); -- The class label its prior probability in the data;
      :lt_PAL_PARAMETER_TBL.INSERT(('PRIORS', NULL, 0.95 , '0'),16);
       
  	
	/* STEP3.2 Run the RandomDecisionTrees Algorithm and Populating the Parameter Table */  
	 CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES (:lt_traindata, :lt_PAL_PARAMETER_TBL, :lt_PAL_RDT_MODEL_TBL, :lt_P4, :lt_P5, :lt_P6) ;
	 select * from :lt_P4; --VAR IMPORTANCE;
	 select * from :lt_P5; --OUT_OF_BAG_ERROR ;
     select * from :lt_P6; --confusion matrix for the traindata;
	
	 select * from :lt_PAL_RDT_MODEL_TBL;
	 
	/*** STEP4 - DEBRIEF and Evaluate **********************************/
	/* Step4.1 Predict with model on test data */
    :lt_PAL_PARAMETER_TBL.DELETE();
    :lt_PAL_PARAMETER_TBL.INSERT(('THREAD_RATIO', NULL, 0.5, NULL),1);
    :lt_PAL_PARAMETER_TBL.INSERT(('VERBOSE',0, NULL, NULL),2);
    :lt_PAL_PARAMETER_TBL.INSERT(('HAS_ID',1, NULL, NULL),3);
   
    CALL _SYS_AFL.PAL_RANDOM_DECISION_TREES_PREDICT (:lt_testdata_PRED, :lt_PAL_RDT_MODEL_TBL,  :lt_PAL_PARAMETER_TBL, :lt_PAL_PREDICT_RESULT) ;
    --Select * from :lt_PAL_PREDICT_RESULT;


   /* Step4.2 Calculate AUC-ROC */
    -- AUC-ROC Calculation;
    lt_PAL_AUC_testdata= 
          select O."ID" as "ID", O."Number_of_mobile_home_policies_num" as "ORIGINAL_LABEL", 
 		      CASE 
 		      WHEN P."SCORE" = '1' Then P."CONFIDENCE" 
 		      ELSE 1-P."CONFIDENCE" END 
 		      as "PROBABILITY"
		from :lt_testdata as O , :lt_PAL_PREDICT_RESULT as P
		where O."ID"=P."ID";
		--Select * from  :lt_PAL_AUC_traindata;
   
    :lt_PAL_PARAMETER_TBL.DELETE();
    :lt_PAL_PARAMETER_TBL.INSERT(('POSITIVE_LABEL', NULL, NULL, '1'),1);
    --Select * from :lt_PAL_PARAMETER_TBL;
   
    CALL _SYS_AFL.PAL_AUC(:lt_PAL_AUC_testdata,:lt_PAL_PARAMETER_TBL,:lt_PAL_AUC_OUT_AUCstats , :lt_PAL_AUC_OUT_ROCdata);
    select 'Test-data AUC' as "NOTE", * from :lt_PAL_AUC_OUT_AUCstats ;
    --select * from :lt_PAL_AUC_OUT_ROCdata ;
  
  
    /* Step4.2 Calculate the confusion matrix and statistics on the test data */
    lt_PAL_CF_testdata= SELECT O.ID, O."Number_of_mobile_home_policies_num" as ORIGINAL_LABEL, P."SCORE" as PREDICTED_LABEL
      from :lt_testdata as O , :lt_PAL_PREDICT_RESULT as P
      where O."ID"=P."ID"; 
    
    :lt_PAL_PARAMETER_TBL.DELETE();
    :lt_PAL_PARAMETER_TBL.INSERT(('BETA',NULL,1,null),1); --F-BETA value;
  
    CALL _SYS_AFL.PAL_CONFUSION_MATRIX(:lt_PAL_CF_testdata ,:lt_PAL_PARAMETER_TBL,:lt_PAL_CF_MATRIX,:lt_PAL_CF_CLASSREPORT);
    select 'Test-data CF' as "NOTE", * from :lt_PAL_CF_MATRIX;
    select * from :lt_PAL_CF_CLASSREPORT;
end;

/*** Inspect the last output, the confusion matrix statistics for the test data ***/
/*** and inspect the second last output, the confusion matrix                   ***/
/*** Out of 238 class '1' events, how many true positives could be predicted?   ***/

/*********************************************************************************************************************************/
/*** Appendix ********************************************************************************************************************/
/*** SQL statements to create the "HANAML_SAMPLES"."INSURANCE_TRAIN" and "HANAML_SAMPLES"."INSURANCE_TRAIN" tables ***************/
/*** Note, both table have the same structure ************************************************************************************/
/*********************************************************************************************************************************/
/*
create column table ""HANAML_SAMPLES""."INSURANCE_TRAIN"( 
     "ID" INT null,
     "Customer_Subtype" NVARCHAR (42) null,
	 "Number_of_houses" INT null,
	 "Avg_size_household" INT null,
	 "Avg_age" NVARCHAR (11) null,
	 "Customer_main_type" NVARCHAR (21) null,
	 "Roman_catholic" NVARCHAR (11) null,
	 "Protestant" INT null,
	 "Other_religion" INT null,
	 "No_religion" INT null,
     "Married" INT null,
	 "Living_together" INT null,
	 "Other_relation" INT null,
	 "Singles" INT null,
	 "Household_without_children" INT null,
	 "Household_with_children" INT null,
	 "High_level_education" INT null,
	 "Medium_level_education" INT null,
     "Lower_level_education" INT null,
	 "High_status" INT null,
     "Entrepreneur" INT null,
	 "Farmer" INT null,
	 "Middle_management" INT null,
	 "Skilled_labourers" INT null,
	 "Unskilled_labourers" INT null,
	 "Social_class_A" INT null,
	 "Social_class_B1" INT null,
	 "Social_class_B2" INT null,
	 "Social_class_C" INT null,
	 "Social_class_D" INT null,
	 "Rented_house" INT null,
	 "Home_owners" INT null,
	 "One_car" INT null,
	 "Two_cars" INT null,
	 "No_car" INT null,
	 "National_Health_Service" INT null,
	 "Private_health_insurance" INT null,
	 "Income_LT_30000" INT null,
	 "Income_30_45000" INT null,
	 "Income_45_75000" INT null,
	 "Income_75_122000" INT null,
	 "Income_GT123000" INT null,
     "Average_income" INT null,
	 "Purchasing_power_class" INT null,
	 "Contribution_private_third_party_insurance" NVARCHAR (11) null,
	 "Contribution_third_party_insurance_firms" INT null,
	 "Contribution_third_party_insurane_agriculture" INT null,
	 "Contribution_car_policies" INT null,
	 "Contribution_delivery_van_policies" INT null,
	 "Contribution_motorcycle_scooter_policies" INT null,
	 "Contribution_lorry_policies" INT null,
	 "Contribution_trailer_policies" INT null,
	 "Contribution_tractor_policies" INT null,
	 "Contribution_agricultural_machines_policies" INT null,
	 "Contribution_moped_policies" INT null,
	 "Contribution_life_insurances" INT null,
	 "Contribution_private_accident_insurance_policies" INT null,
	 "Contribution_family_accidents_insurance_policies" INT null,
	 "Contribution_disability_insurance_policies" INT null,
	 "Contribution_fire_policies" INT null,
	 "Contribution_surfboard_policies" INT null,
	 "Contribution_boat_policies" INT null,
	 "Contribution_bicycle_policies" INT null,
	 "Contribution_property_insurance_policies" INT null,
	 "Contribution_social_security_insurance_policies" INT null,
	 "Number_of_private_third_party_insurance" INT null,
	 "Number_of_third_party_insurance_firms" INT null,
	 "Number_of_third_party_insurane_agriculture" INT null,
	 "Number_of_car_policies" INT null,
	 "Number_of_delivery_van_policies" INT null,
	 "Number_of_motorcycle_scooter_policies" INT null,
	 "Number_of_lorry_policies" INT null,
	 "Number_of_trailer_policies" INT null,
	 "Number_of_tractor_policies" INT null,
	 "Number_of_agricultural_machines_policies" INT null,
	 "Number_of_moped_policies" INT null,
	 "Number_of_life_insurances" INT null,
	 "Number_of_private_accident_insurance_policies" INT null,
	 "Number_of_family_accidents_insurance_policies" INT null,
	 "Number_of_disability_insurance_policies" INT null,
	 "Number_of_fire_policies" INT null,
	 "Number_of_surfboard_policies" INT null,
	 "Number_of_boat_policies" INT null,
	 "Number_of_bicycle_policies" INT null,
	 "Number_of_property_insurance_policies" INT null,
	 "Number_of_social_security_insurance_policies" INT null,
	 "Number_of_mobile_home_policies_num" INT null );
*/





	 
