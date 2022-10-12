using cpm.ml as my from '../db/data-model';

service CatalogService {
    entity Stations as projection on my.Stations;
    entity Prices as projection on my.Prices;
    entity Prediction_Results as projection on my.PAL_ADDITIVE_MODEL_PREDICT_FORECAST_RESULT_TBL_1;

    function Prices_Predict() returns Boolean; 
    function Model_Train() returns Boolean;
}