# Getting Started

Welcome to your new project.

It contains these folders and files, following our recommended project layout:

File or Folder | Purpose
---------|----------
`app/` | content for UI frontends goes here
`db/` | your domain models and data go here
`srv/` | your service models and code go here
`package.json` | project metadata and configuration
`readme.md` | this getting started guide


## Project Setup Guidelines

1. Create a database user (PAL_ACCESS_GRANTOR) and grant the PAL procedure previleges to this database user in **HANA Database Explorer** before your first deployment

```
CREATE USER PAL_ACCESS_GRANTOR PASSWORD <password> NO FORCE_FIRST_PASSWORD_CHANGE;

-- create role
CREATE ROLE "data::external_access_g";
CREATE ROLE "data::external_access";

GRANT "data::external_access_g", "data::external_access" TO PAL_ACCESS_GRANTOR WITH ADMIN OPTION; 

GRANT AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION, AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION to "data::external_access_g";
GRANT AFL__SYS_AFL_AFLPAL_EXECUTE, AFL__SYS_AFL_AFLPAL_EXECUTE_WITH_GRANT_OPTION to "data::external_access";
```

2. Create a user provided service in the space where your CAP application is running with the above database username and password
```
{
    "password": "Your password",
    "tags": [
        "hana"
    ],
    "user": "PAL_ACCESS_GRANTOR"
}
```
![Screenshot 2022-08-04 at 13 27 12](https://media.github.tools.sap/user/12569/files/81181c00-2555-4201-aca6-ce46044ed0bd)

3. Adapt the MTA.YAML file with your user provided service instance

```
- name: cross-container-service-1
  type: org.cloudfoundry.existing-service
  parameters:
    service-name: ml_hana_bas_ups --> Your User Provided Service Name
  properties:
    the-service-name: ${service-name}
    
```
4. Import the generated database design time artefacts into DB module of your CAP project
Let's look at required files of DB module: 
- Synonym file (.hdbsynonym): **access a different database schema**, e.g., call the PAL procedure for massive prediction from schema '_SYS_AFL'
```
{
  "SYSAFL::PALMASSIVEADDITIVEMODELANALYSIS": {
    "target": {
      "object": "PAL_MASSIVE_ADDITIVE_MODEL_ANALYSIS",
      "schema": "_SYS_AFL"
    }
  },
  "SYSAFL::PALMASSIVEADDITIVEMODELPREDICT": {
    "target": {
      "object": "PAL_MASSIVE_ADDITIVE_MODEL_PREDICT",
      "schema": "_SYS_AFL"
    }
  }
}
```

- Grant file (.hdbgrants): **grant the roles of user provided service to the HDI runtime user of your CAP project**
```
{
    "ml_hana_bas_ups": {   --> Your user provided service name
        "object_owner": {
            "roles": [
                "data::external_access_g".      --> Roles created in Step 1 to call PAL procedure
            ]
        },
        "application_user": {
            "roles": [
                "data::external_access"
            ]
        }
    }
}
```
- PAL procedures (.hdbprocedure): **generated via hana-ml in Jupyter Notebook**

Training procedure for ML model: base_additivemodelforecast1_fit
Prediction procedure: base_additivemodelforecast1_predict

What important: **Training procedure needs to be called before prediction procedure. Table structures used in both the prodecures shall be created under CAP cds and available when both the procedures are deployed.**

- Tables in CAP cds file (.cds): **need to be defined before deloyment of your DB module** Please refer to the data-model.cds (https://github.tools.sap/D064237/ml_hana_bas/blob/main/db/data-model.cds) file in this repository. 

Traning data for ML model:
```
entity Stations { 
  ...  
}

entity Prices {
  ...  
}
```
Table for PAL procedure:
```
entity PAL_ADDITIVE_MODEL_ANALYSIS_HOLIDAY  {
  ... 
}

entity  PAL_ADDITIVE_MODEL_ANALYSIS_MODEL {
  ...
}

entity PAL_ADDITIVE_MODEL_FORECAST_PREDICT_DECOMPOSITION_TBL_1 {
   ...
}

entity PAL_ADDITIVE_MODEL_FORECAST_PREDICT_ERROR_TBL_1 {
   ...
}
```
Table for prediction results:
```
entity PAL_ADDITIVE_MODEL_PREDICT_FORECAST_RESULT_TBL_1{
   ...
}

entity FUEL_PRICES_RNK_TEST {
   ...
}
```
5. Add the Javascript class under the Node.js module to call the prediction procedure

First, add the library "sap-hdb-promisfied" in the package.json file (https://github.tools.sap/D064237/ml_hana_bas/blob/main/package.json), which is required to call database procedures in Javascript.
```
"dependencies": {
        "@sap/cds": "^5",
        "express": "^4",
        "hdb": "^0.18.3",
        "sap-hdb-promisfied": "^2.202205.1"
    }
```
Secondly, define service functions under oData service of your CAP project to enable user to call procedures inside these functions.
```
service CatalogService {
    entity Stations as projection on my.Stations;
    entity Prices as projection on my.Prices;

    function Prices_Predict() returns Boolean; --> Call prediction procedure and generate prediction results
    function Model_Train_Wrapper() returns Boolean; --> Call training procedure and generate ML models
}
```
Finally, implement the logic using Javascript to generate prediction results. Example of codes are available in this JS script
(https://github.tools.sap/D064237/ml_hana_bas/blob/main/srv/cat-service.js).

6. Call functions to generate ML model and make predictions
First, you can call the function Model_Train_Wrapper from your oData service to generate the ML model. Then, you can call the function Prices_Prediction to give you all prediction results back. 
![Screenshot 2022-08-08 at 11 42 25](https://media.github.tools.sap/user/12569/files/5ee5a768-3c5e-4331-81e9-8f4f3f73b1a7)


## Learn More

Learn more at https://cap.cloud.sap/docs/get-started/.
