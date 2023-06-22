namespace hana.ml;

context Fit {
  entity ModelHanaMlConsPalMassiveAdditiveModelAnalysis {
    group_id      : String(100);
    row_index     : Integer;
    model_content : LargeString;
  }

  entity Output1PalMassiveAdditiveModelAnalysis {
    group_id        : String(100);
    error_timestamp : String(100);
    errorcode       : Integer;
    message         : String(200);
  }
}

context Predict {
  entity Output0PalMassiveAdditiveModelPredict {
    group_id           : String(100);
    price_at_timestamp : Timestamp;
    yhat               : Double;
    yhat_lower         : Double;
    yhat_upper         : Double;
  }

  entity Output1PalMassiveAdditiveModelPredict {
    group_id           : String(100);
    price_at_timestamp : Timestamp;
    trend              : Double;
    seasonal           : LargeString;
    holiday            : LargeString;
    exogenous          : LargeString;
  }

  entity Output2PalMassiveAdditiveModelPredict {
    group_id        : String(100);
    error_timestamp : String(100);
    errorcode       : Integer;
    message         : String(200);
  }

}

@cds.persistence.exists
entity POINTS_OF_SALES {
  key uuid         : String;
      name         : String @Common.Label: '{i18n>PosLabel.name}';
      brand        : String @Common.Label: '{i18n>PosLabel.brand}';
      street       : String @Common.Label: '{i18n>PosLabel.street}';
      house_number : String @Common.Label: '{i18n>PosLabel.house_number}';
      post_code    : String @Common.Label: '{i18n>PosLabel.post_code}';
      city         : String @Common.Label: '{i18n>PosLabel.city}';
      latitude     : Double @Common.Label: '{i18n>PosLabel.latitude}';
      longitude    : Double @Common.Label: '{i18n>PosLabel.longitude}';
}

@cds.persistence.exists
entity TEST_RNK_2209 {
  PRICE_AT_TIMESTAMP : Timestamp;
  STATION_UUID       : String(50);
  E5                 : Double;
}

view History_Forecast as
  select
    key tr.STATION_UUID       as uuid,
        tr.PRICE_AT_TIMESTAMP as date,
        round(
          tr.E5, 3
        )                     as price_hist       : Decimal(10, 3),
        round(
          ou.yhat, 3
        )                     as price_fcst       : Decimal(10, 3),
        round(
          ou.yhat_lower, 3
        )                     as price_fcst_lower : Decimal(10, 3),
        round(
          ou.yhat_upper, 3
        )                     as price_fcst_upper : Decimal(10, 3)
  from TEST_RNK_2209 as tr
  inner join Predict.Output0PalMassiveAdditiveModelPredict as ou
    on tr.STATION_UUID = ou.group_id and tr.PRICE_AT_TIMESTAMP = ou.price_at_timestamp
  order by
    uuid,
    date;
