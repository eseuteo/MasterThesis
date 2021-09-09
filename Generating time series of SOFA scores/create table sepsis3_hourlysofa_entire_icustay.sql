

create table if not exists mimiciii.sepsis3_hourlysofa_entire_icustay
(
	hadm_id int4,
	icustay_id int4,
	intime timestamp(0),
	outtime timestamp(0),
	suspection_of_infection_time timestamp(0),
	window_start_time  timestamp(0),
	window_end_time  timestamp(0),
	calculation_time timestamp(0),
	sofa_score integer,
  respiration integer,
  coagulation integer,
  liver integer,
  cardiovascular integer,
  cns integer,
  renal integer,
  subject_id int4

);


create index if not exists sepsis3_hourlysofa_entirestay_icustay_id_idx
	on mimiciii.sepsis3_hourlysofa_entire_icustay (icustay_id);