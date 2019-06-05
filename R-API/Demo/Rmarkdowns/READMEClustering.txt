To run the Clustering Demo, you would need to load pre-requesites libraries and  install the hana.ml.r package
  
1. Check that the following packages are installed:
  
  library(data.table)
  library(R6)
  library(lintr)
  library(testthat)
  library(futile.logger)
  library(sets)
  library(RODBC)
  library(ids)

 2. Install the latest hana.ml.r package in R environment
 
 3. For dataset IRIS, 
		. Schema:
			 CREATE COLUMN TABLE "DEVUSER"."IRIS" ("ID" INTEGER CS_INT,
			 "SEPALLENGTHCM" DOUBLE CS_DOUBLE,
			 "SEPALWIDTHCM" DOUBLE CS_DOUBLE,
			 "PETALLENGTHCM" DOUBLE CS_DOUBLE,
			 "PETALWIDTHCM" DOUBLE CS_DOUBLE,
			 "SPECIES" VARCHAR(15))
		. Get a copy to your own HANA instance from https://archive.ics.uci.edu/ml/datasets/iris
		. create the DSN to access your HANA instance
		. modify the DSN, user and password from the ClusteringDemo.Rmd file

4. Run ClusteringDemo.Rmd
	Run ClusteringDemo.Rmd from Rstudio.