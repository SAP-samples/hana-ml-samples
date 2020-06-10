#Author: Yannick Schaper

#Description: 
#See the Hands-On Tutorial: https://blogs.sap.com/2020/06/10/hands-on-tutorial-becoming-the-chief-data-cook-with-rstudio-and-sap-hana/?update=publish
#
#Requirements: 
#- SAP HANA 2 SPS04
#- R Version: 3.6.3
#
#Output
#- ODBC connection
#- Joined datasets
#- SQL query

#---------------------------------------------------------------------------------------------------------------------------------------------------

#install packages
install.packages("DBI")
install.packages("odbc")
install.packages("dbplyr")
install.packages("dplyr")
#load packages
library(DBI)
library(odbc)
library(dbplyr)
library(dplyr)

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# simulate data
set.seed(27)
df1 = data.frame(CUSTOMERID = c(1:10), PRODUCT = sample(c("Cookie Dough","Chocolate Fudge Brownie",
                                                        "Peanut Butter Cup","Strawberry Cheesecake",
                                                        "Half Baked","Salted Caramel Brownie"),
                                                        10, replace = TRUE))
df2 = data.frame(CUSTOMERID = c(2, 5, 6, 8, 10), SCORE = c(4, 5, 3, 2, 5)) 

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#create connection through ODBC
#Connection through dpblyr (CRAN)
con_dbplyr <- dbConnect(odbc::odbc(), dsn = '', uid = "", pwd = "")

#Push data into SAP HANA
dbWriteTable(con_dbplyr, "DF1", df1, overwrite = TRUE)
dbWriteTable(con_dbplyr, "DF2", df2, overwrite = TRUE)

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#use tbl() to create a reference to the data
DF1 <- tbl(con_dbplyr, "DF1")
DF2 <- tbl(con_dbplyr, "DF2")

#print the results
DF1
DF2

#execute an inner join with the key set to CUSTOMERID
DFJOINED <- inner_join(DF1, DF2, key = "CUSTOMERID")

#print the resulting table to the console
DFJOINED

#have a look at the sql query
show_query(DFJOINED)

#collect data into local enviornment
DFJOINED_LOCAL <- collect(DFJOINED)

#set a filter on the joined table
FILTER <- filter(DFJOINED, SCORE > 4)

#show sql query
show_query(FILTER)

#collect data into local enviornment
FILTER_LOCAL <- collect(FILTER)






















