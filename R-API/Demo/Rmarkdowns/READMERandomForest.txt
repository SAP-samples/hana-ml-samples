To run the Random Forest Demo, you would need to load pre-requesites libraries and  install the hana.ml.r package
  
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
 
 3. For dataset BOSTON:
		. Table Schema:
			 CREATE COLUMN TABLE BOSTON(
			"CRIM" DECIMAL(12,5) CS_FIXED,
			"ZN" DECIMAL(7,3) CS_FIXED,
			"INDUS" DECIMAL(7,2) CS_FIXED,
			"CHAS" SMALLINT CS_INT, "NOX" DECIMAL(10,4) CS_FIXED,
			"RM" DECIMAL(8,3) CS_FIXED,
			"AGE" DECIMAL(7,3) CS_FIXED,
			"DIS" DECIMAL(11,4) CS_FIXED,
			"RAD" TINYINT CS_INT,
			"TAX" SMALLINT CS_INT,
			"PTRATIO" DECIMAL(6,2) CS_FIXED,
			"BLACK" DECIMAL(9,3) CS_FIXED,
			"LSTAT" DECIMAL(7,2) CS_FIXED,
			"MEDV" DECIMAL(6,2) CS_FIXED,
			"ID" INTEGER
			);
		. Get the dataset from the package MAAS by doing require(MASS)
		. create the DSN to access your HANA instance
		. modify the DSN, user and password from the RandomForestDemo.Rmd file
	

4. Run RandomForest.Rmd
	Run RandomForest.Rmd from Rstudio