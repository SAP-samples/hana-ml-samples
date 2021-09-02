AbapCatalog.sqlViewName: 'Z_SFLI_TRAIN'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'Training Data for SFLIGHT Scenario'
define view Z_SFLIGHT_TRAIN as select from sflight {
  key cast(carrid     as s_carr_id)  as carrid,
  key cast(connid     as s_conn_id)  as connid,
  key cast(fldate     as s_date)     as fldate,
      cast(price      as s_price)    as price,
      cast(currency   as s_currcode) as currency,
      cast(planetype  as s_planetye) as planetype,
      cast(seatsmax   as s_seatsmax) as seatsmax,
      cast(seatsocc   as s_seatsocc) as seatsocc,
      cast(paymentsum as s_sum)      as paymentsum,
      cast(seatsmax_b as s_smax_b)   as seatsmax_b,
      cast(seatsocc_b as s_socc_b)   as seatsocc_b,
      cast(seatsmax_f as s_smax_f)   as seatsmax_f,
      cast(seatsocc_f as s_socc_f)   as seatsocc_f
}
-- the training dataset is based on dates before or equal to '20190228'
where fldate <= '20190228'

-----------------------------------------------------------------------

@AbapCatalog.sqlViewName: 'Z_SFLI_PRED'
@AbapCatalog.compiler.compareFilter: true
@AbapCatalog.preserveKey: true
@AccessControl.authorizationCheck: #CHECK
@EndUserText.label: 'Prediction Data for SFLIGHT Scenario'
define view Z_SFLIGHT_PREDICT as select from sflight {
  key cast(carrid     as s_carr_id)  as carrid,
  key cast(connid     as s_conn_id)  as connid,
  key cast(fldate     as s_date)     as fldate,
      cast(price      as s_price)    as price,
      cast(currency   as s_currcode) as currency,
      cast(planetype  as s_planetye) as planetype,
      cast(seatsmax   as s_seatsmax) as seatsmax,
      cast(seatsocc   as s_seatsocc) as seatsocc,
      cast(paymentsum as s_sum)      as paymentsum,
      cast(seatsmax_b as s_smax_b)   as seatsmax_b,
      cast(seatsocc_b as s_socc_b)   as seatsocc_b,
      cast(seatsmax_f as s_smax_f)   as seatsmax_f,
      cast(seatsocc_f as s_socc_f)   as seatsocc_f
}
-- the prediction dataset is based on dates after '20190228'
where fldate > '20190228'
