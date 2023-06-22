using hana.ml as hanaml from '../db/hana-ml-cds-hana-ml-base-pal-massive-additive-model-analysis';

service CatalogService {
    @readonly
    entity ModelHanaMlConsPalMassiveAdditiveModelAnalysis as projection on hanaml.Fit.ModelHanaMlConsPalMassiveAdditiveModelAnalysis;

    @readonly
    entity Output1PalMassiveAdditiveModelAnalysis         as projection on hanaml.Fit.Output1PalMassiveAdditiveModelAnalysis;

    @readonly
    entity Output0PalMassiveAdditiveModelPredict          as projection on hanaml.Predict.Output0PalMassiveAdditiveModelPredict;

    @readonly
    entity Output1PalMassiveAdditiveModelPredict          as projection on hanaml.Predict.Output1PalMassiveAdditiveModelPredict;

    @readonly
    entity Output2PalMassiveAdditiveModelPredict          as projection on hanaml.Predict.Output2PalMassiveAdditiveModelPredict;

    entity TEST_RNK_2209                                  as projection on hanaml.TEST_RNK_2209;
    entity History_Forecast                               as projection on hanaml.History_Forecast;
    entity POINTS_OF_SALES                                as projection on hanaml.POINTS_OF_SALES;

    function Prices_Predict() returns Boolean; 
    function Model_Train() returns Boolean;
}
