create or replace table t_order_result
(
    order_no   varchar(64) default '' comment 'order no',
    currency   varchar(32) default '' comment 'currency',
    amount     float       default 0 comment 'amount',
    rmb_amount float       default 0 comment 'rmb amount',
    time       timestamp comment 'time'
);


create or replace table t_doctor_relate_patient_name
(
    doctor_uin  int,
    patient_uin int,
    name        varchar(10),
    primary key (doctor_uin, patient_uin)
);
