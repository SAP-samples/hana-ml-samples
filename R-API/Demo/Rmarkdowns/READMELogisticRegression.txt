To run the LogisticRegression R markdown demo, you would need to load pre-requesites libraries and  install the hana.ml.r package after setting approripate .libpath.
  
1. Check that the following packages are installed:
  
  library(data.table)
  library(R6)
  library(lintr)
  library(testthat)
  library(futile.logger)
  library(sets)
  library(RODBC)
  library(ids)

 2. Load the latest hana.ml.r package in R environment
 
 3. We are using the bank dataset dataset for this demo from UCI. 

		Schema:
                CREATE COLUMN TABLE DBM_RTEST2_TBL (ID INTEGER, 
                AGE INTEGER, 
                JOB VARCHAR(256), 
                MARITAL VARCHAR(100), 
                EDUCATION VARCHAR(256), 
                DBM_DEFAULT VARCHAR(100), 
                HOUSING VARCHAR(100), 
                LOAN VARCHAR(100), 
                CONTACT VARCHAR(100), 
                DBM_MONTH VARCHAR(100), 
                DAY_OF_WEEK VARCHAR(100), 
                DURATION DOUBLE, 
                CAMPAIGN INTEGER, 
                PDAYS INTEGER, 
                PREVIOUS INTEGER, 
                POUTCOME VARCHAR(100), 
                EMP_VAR_RATE DOUBLE, 
                CONS_PRICE_IDX DOUBLE, 
                CONS_CONF_IDX DOUBLE, 
                EURIBOR3M DOUBLE, 
                NREMPLOYED DOUBLE, 
                LABEL VARCHAR(10) 
                )

Get a copy to your own HANA instance from 
https://archive.ics.uci.edu/ml/datasets/bank+marketing#.
		. create the DSN to access your HANA instance
		. modify the DSN, user and password from the LogisitcRegressionDemo.Rmd file

Run LogisticRegressionDemo.Rmd from Rstudio.
