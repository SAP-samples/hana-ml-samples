namespace cpm.ml;

entity Stations {
  key uuid : String;
  name : String; 
  brand : String;
  street : String;
  house_number : String; 
  post_code : String; 
  city : String;
  latitude : Decimal;
  longitude : Decimal;
}

entity Prices {
  date : Timestamp;
  station_uuid : String (50);
  diesel : Double;
  e5 : Double;
  e10 : Double; 
  dieselchange : Double;
  e5change : Double;
  e10change : Double;
}

entity PAL_ADDITIVE_MODEL_ANALYSIS_HOLIDAY  {
    GROUP_IDXXX : Integer;
    TS : Timestamp;
    NAME : String(255);
    LOWER_WINDOW : Integer; 
    UPPER_WINDOW : Integer;
}

entity  PAL_ADDITIVE_MODEL_ANALYSIS_MODEL {
    GROUP_ID : String(100);
    ROW_INDEX : Integer;
    MODEL_CONTENT : String(5000);
}

entity PAL_ADDITIVE_MODEL_FORECAST_PREDICT_DECOMPOSITION_TBL_1 {
    GROUP_ID : String(100);
    DATE : Timestamp;
    TREND : Double;
    SEASONAL: LargeString;
    HOLIDAY: LargeString;
    EXOGENOUS: LargeString;
}

entity PAL_ADDITIVE_MODEL_FORECAST_PREDICT_ERROR_TBL_1 {
    GROUP_ID : String(100);
    ERROR_TIMESTAMP : String(100);
    ERRORCODE : Integer;
    MESSAGE: String(200);
}

entity PAL_ADDITIVE_MODEL_PREDICT_FORECAST_RESULT_TBL_1{
    GROUP_ID : String(100);
    DATE : Timestamp;
    YHAT : Double;
    YHAT_LOWER : Double;
    YHAT_UPPER   : Double; 
}

entity FUEL_PRICES_RNK_TEST {
  date : Timestamp;
  station_uuid : String (50);
  diesel : Double;
  e5 : Double;
  e10 : Double; 
  dieselchange : Double;
  e5change : Double;
  e10change : Double;
}