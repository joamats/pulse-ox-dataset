-- v1: 2021.09.07
-- There are three tables (careplangeneral, respiratorycare, respiratorycharting) existing ventaliation information
-- While the info of respiratorycare is not accurate. So, we will get the detail from careplangeneral table

drop table if exists `db_name.pivoted_vent_part1_eicu`;
create table `db_name.pivoted_vent_part1_eicu` as

with ventaliation_info as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, case 
	when cplitemvalue like 'Intubated%' 
	or cplitemvalue = 'Ventilated - chronic dependency'
	or cplitemvalue = 'Ventilated - with daily extubation evaluation'
	or cplitemvalue = 'Ventilated - with no daily extubation trial'
	or cplitemvalue = 'Non-invasive ventilation' then 1
    else 0 end as vent_flag
	-- Intubated/nasal ETT	            | 335
	-- Intubated/nasal ETT - difficult	| 52
	-- Intubated/oral ETT	            | 59566
	-- Intubated/oral ETT - difficult	| 798
	-- Intubated/trach-acute	        | 4829
	-- Intubated/trach-chronic	        | 4993
	-- Ventilated - chronic dependency	                | 3105
	-- Ventilated - with daily extubation evaluation	| 51862
 	-- Ventilated - with no daily extubation trial	    | 14907  
    -- Non-invasive ventilation	                        | 26836

    -- Ventilated - rapid wean/extubation	      | 5705
	-- Not intubated/normal airway	              | 206795
	-- Not intubated/partial airway obstruction	  | 1543
	-- Spontaneous - adequate	                  | 190809
	-- Spontaneous - tenuous	                  | 32587
	--                                            | 14896	
	from `physionet-data.eicu_crd.careplangeneral`
	where cplgroup in ('Airway', 'Ventilation') 
	and cplitemvalue != ''
)

, ventilation_00 as (
	select patientunitstayid
	, sum(vent_flag) as num
	from ventaliation_info
	group by patientunitstayid
)

, ventilation_01 as ( -- drop patientunitstayid didn't have ventaliation
	select patientunitstayid
	, cplitemoffset
	, sum(vent_flag) as num
	from ventaliation_info
	where patientunitstayid not in (
		select patientunitstayid 
		from ventilation_00 
		where num = 0 
		group by patientunitstayid
		)
	group by patientunitstayid
	, cplitemoffset
)

, ventilation_02 as (
	select vi.cplgeneralid
	, vi.patientunitstayid
	, vi.activeupondischarge
	, vi.cplitemoffset
	, vi.cplgroup
	, vi.cplitemvalue
	, vi.vent_flag
	, ROW_NUMBER()
	over (partition by vi.patientunitstayid, vi.cplitemoffset order by vi.vent_flag desc) as flag
	from ventaliation_info vi 
	inner join ventilation_01 v0
	on vi.patientunitstayid = v0.patientunitstayid
	and vi.cplitemoffset = v0.cplitemoffset
	where v0.num >= 1
	and vi.vent_flag = 0
)

-- drop the same cplitemoffset rows of non-ventiliation, existing ventiliation and non-ventiliation
, ventilation_0 as ( 
	select vi.cplgeneralid
	, vi.patientunitstayid
	, vi.activeupondischarge
	, vi.cplitemoffset
	, vi.cplgroup
	, vi.cplitemvalue
	, vi.vent_flag
	from ventaliation_info vi
	where vi.cplgeneralid not in (select cplgeneralid from ventilation_02 where flag = 1)
)

-- solving the same cplitemoffset rows of more than two different ventiliation
-- remain one rows
, ventilation_10 as (
	select cplgeneralid
	, ROW_NUMBER() 
	OVER (partition by patientunitstayid, cplitemoffset order by cplitemvalue) as rn
	from ventilation_0
	where vent_flag = 1
)

select *
from ventilation_0
where cplgeneralid not in (select cplgeneralid from ventilation_10 where rn > 1)
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_part2_eicu`;
create table `db_name.pivoted_vent_part2_eicu` as

-- if existing: delete the first rows of non-ventiliation
with ventilation_20 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset) as rn
	from `db_name.pivoted_vent_part1_eicu`
)

, ventilation_21 as (
	select *
	from ventilation_20
	where patientunitstayid in (
		select patientunitstayid
		from ventilation_20
		where rn = 1 and vent_flag = 0
		group by patientunitstayid
	)
)

, ventilation_22 as (
	select *
	from ventilation_20
	where cplgeneralid not in (select cplgeneralid from ventilation_21)
)

-- ventilation_21: delete the first rows of non-ventiliation
, ventilation_210 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, LAG(vent_flag, 1) OVER (partition by patientunitstayid order by cplitemoffset) as vent_flag_new
	from ventilation_21
	-- order by patientunitstayid
	-- , cplitemoffset
)

, ventilation_211 as (
	select *
	, vent_flag_new - vent_flag as del_flag
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset) as rn
	from ventilation_210
	where vent_flag_new - vent_flag = -1
)

, ventilation_212 as (
	select *
	from ventilation_211
	where rn = 1
)

, ventilation_213 as (
	select v21.*
	from ventilation_21 v21
	inner join ventilation_212 v212
	on v21.patientunitstayid = v212.patientunitstayid
	and v21.cplitemoffset >= v212.cplitemoffset
)

, ventilation_2 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_213
	union all
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_22 
)

select distinct *
from ventilation_2
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_part34_eicu`;
create table `db_name.pivoted_vent_part34_eicu` as

-- delete the same cplitemoffset with different types of non-ventilation, remain one row
with ventilation_30 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, ROW_NUMBER()
	over (partition by patientunitstayid, cplitemoffset order by cplitemvalue desc) as rn
	from `db_name.pivoted_vent_part2_eicu`
	where vent_flag = 0
)

, ventilation_31 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from `db_name.pivoted_vent_part2_eicu`
	where vent_flag != 0
)

, ventilation_3 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_30
	where rn = 1
	union all
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_31
)

-- existing some patientunitstayid didn't know the endtime
-- Assume that it ends after 1h
, ventilation_40 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from (select distinct * from ventilation_3)
)

, ventilation_41 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag 
	from ventilation_40
	where rn = 1
	and vent_flag = 1
	and activeupondischarge is false
)

, ventilation_411 as (
    select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_41
	union all
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset + 60 as cplitemoffset
	, cplgroup
	, 'Spontaneous - adequate' as cplitemvalue
	, 0 as vent_flag
	from ventilation_41  
)

, ventilation_42 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag 
	from ventilation_40
	where cplgeneralid not in (select cplgeneralid from ventilation_41)
)

, ventilation_4 as (
	select *
	from ventilation_411
	union all
	select *
	from ventilation_42
)

select distinct *
from ventilation_4
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_part56_eicu`;
create table `db_name.pivoted_vent_part56_eicu` as

-- existing some patients : the last two rows were active ventilation and active non-ventilation
-- we will handle this situation : assume that patient finish ventilation before start non-ventilation
with ventilation_50 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from `db_name.pivoted_vent_part34_eicu`
)

, ventilation_500 as (
	select *
	, case
	when rn = 1 and vent_flag = 0 then 1
	else 0 end as flag
	from ventilation_50
)

, ventilation_501 as (
	select *
	, case
	when rn = 2 and vent_flag = 1 and activeupondischarge is true then 1
	else 0 end as flag
	from ventilation_50
)

, ventilation_502 as (
	select patientunitstayid
	from ventilation_500
	where patientunitstayid in (
		select patientunitstayid 
		from ventilation_501 
		where flag=1 
		group by patientunitstayid
		)
	and flag = 1
	group by patientunitstayid
)

, ventilation_510 as ( --  needing to modify
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from ventilation_50
	where patientunitstayid in (select * from ventilation_502)
)

, ventilation_51 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag	
	from ventilation_510
	where rn > 1
	union all
	select cplgeneralid
	, patientunitstayid
	, false as activeupondischarge
	, cplitemoffset
	, cplgroup
	, 'Spontaneous - adequate' as cplitemvalue
	, 0 as vent_flag	
	from ventilation_510
	where rn = 1	
)


, ventilation_52 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_50
	where patientunitstayid not in (select * from ventilation_502)
)

, ventilation_5 as (
	select distinct *
	from (
		select *
		from ventilation_51
		union all
		select *
		from ventilation_52
	)
)

-- handling with tha last row is activeupondischarge = True and vent_flag = 1
, ventilation_60 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by cplitemoffset desc) as rn
	from ventilation_5
)

, ventilation_610 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	, rn
	from ventilation_60 
	where patientunitstayid in (
		select patientunitstayid
		from ventilation_60
		where rn = 1
		and vent_flag = 1
		and activeupondischarge is true
		group by patientunitstayid
		)
)

, ventilation_61 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_610
	union all
	select vt.cplgeneralid
	, vt.patientunitstayid
	, false as activeupondischarge
	, icud.unitdischargeoffset as cplitemoffset
	, 'Airway' as cplgroup
	, 'Spontaneous - adequate' as cplitemvalue
	, 0 as vent_flag
	from ventilation_610 vt
	left join `physionet-data.eicu_crd_derived.icustay_detail` icud 
	on vt.patientunitstayid = icud.patientunitstayid
	where vt.rn = 1
)

, ventilation_62 as (
	select cplgeneralid
	, patientunitstayid
	, activeupondischarge
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, vent_flag
	from ventilation_60
	where cplgeneralid not in (
		select cplgeneralid
		from ventilation_610
		)
)

, ventilation_6 as (
	select *
	from ventilation_61
	union all
	select *
	from ventilation_62
)

select distinct *
from ventilation_6
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_eicu`;
create table `db_name.pivoted_vent_eicu` as

-- get start and end time of ventilation
with ventilation_70 as (
	select patientunitstayid
	, activeupondischarge
	, cplitemoffset as starttime
	, lead(cplitemoffset, 1) OVER (partition by patientunitstayid order by cplitemoffset) as endtime
	, cplgroup
	, cplitemvalue
	, vent_flag
	, lead(vent_flag, 1) OVER (partition by patientunitstayid order by cplitemoffset) as vent_flag_new
	from `db_name.pivoted_vent_part56_eicu`
)

, ventilation_701 as (
	select *
	, vent_flag - vent_flag_new as flag
	from ventilation_70
	where vent_flag = 1
	and vent_flag - vent_flag_new != -1
)

, ventilation_71 as (
	select patientunitstayid
	, starttime
	, endtime
	from ventilation_701
	where flag = 1
)

, ventilation_720 as (
	select distinct *
	from (
		select patientunitstayid
		, starttime as cplitemoffset
		from ventilation_701
		where flag = 0
		union all
		select patientunitstayid
		, endtime as cplitemoffset
		from ventilation_701
		where flag = 0
	)
)

, ventilation_721 as (
	select patientunitstayid
	, cplitemoffset
	, count(cplitemoffset) as num
	from ventilation_720
	group by patientunitstayid
	, cplitemoffset
)

, ventilation_72 as (
	select patientunitstayid
	, cplitemoffset
	from ventilation_721
	where num = 1
)

, ventilation_730 as (
	select distinct *
	from (
		select patientunitstayid
		, starttime as cplitemoffset
		from ventilation_71
		union all
		select patientunitstayid
		, endtime as cplitemoffset
		from ventilation_71
		union all
		select patientunitstayid
		, cplitemoffset
		from ventilation_72
	)
)

, ventilation_731 as (
	select patientunitstayid
	, cplitemoffset
	, count(cplitemoffset) as num
	from ventilation_730
	group by patientunitstayid
	, cplitemoffset
)

, ventilation_732 as (
	select patientunitstayid
	, cplitemoffset
	from ventilation_731
	where num = 1
	order by cplitemoffset
)

, ventilation_733 as (
	select patientunitstayid
	, cplitemoffset as starttime
    , lead(cplitemoffset, 1) OVER (partition by patientunitstayid order by cplitemoffset) as endtime
    from ventilation_732
)

, ventilation_734 as (
	select *
	, ROW_NUMBER()
	OVER (partition by patientunitstayid order by starttime) as rn
	from ventilation_733
	where endtime is not null
)

select patientunitstayid
, starttime
, endtime
from ventilation_734
where mod(rn,2) = 1
order by patientunitstayid
, starttime;


-- drop temporal tables
drop table if exists `db_name.pivoted_vent_part1_eicu`;
drop table if exists `db_name.pivoted_vent_part2_eicu`;
drop table if exists `db_name.pivoted_vent_part34_eicu`;
drop table if exists `db_name.pivoted_vent_part56_eicu`;





/*
-- v2
-- by xiaoli liu, 2022.01.10
-- need to write later
-- can't process multiple subquery, so we split them and drop them finally
drop table if exists `db_name.ventaliation_info_time_0_eicu`;
create table `db_name.ventaliation_info_time_0_eicu` as

with ventaliation_info_initial as (
    select ce.patientunitstayid
	, activeupondischarge
    , hospitaldischargeoffset
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, case 
	when cplitemvalue in (
        'Intubated/nasal ETT'
        , 'Intubated/oral ETT'
        , 'Intubated/trach-acute'
        , 'Intubated/trach-chronic'
        , 'Ventilated - chronic dependency'
        , 'Ventilated - with daily extubation evaluation'
        , 'Ventilated - with no daily extubation trial'                
    ) then 'invasive'
    when cplitemvalue in (
        'Not intubated/normal airway'
        , 'Not intubated/partial airway obstruction'
        , 'Non-invasive ventilation'
        , 'Spontaneous - tenuous'
    ) then 'non-invasive' else null end as vent_type
    , case
    when cplitemvalue = 'Spontaneous - adequate' then 0 else 1 end as vent_start_flag   
    from `physionet-data.eicu_crd.careplangeneral` ce 
    inner join `physionet-data.eicu_crd_derived.icustay_detail` icud 
    on ce.patientunitstayid = icud.patientunitstayid
    and ce.cplitemoffset <= icud.hospitaldischargeoffset -- we drop the info out of the same icu admitted
	where cplgroup in ('Airway', 'Ventilation') 
	and cplitemvalue in (
        'Intubated/nasal ETT'
        , 'Intubated/oral ETT'
        , 'Intubated/trach-acute'
        , 'Intubated/trach-chronic'
        , 'Not intubated/normal airway'
        , 'Not intubated/partial airway obstruction'
        , 'Non-invasive ventilation'
        , 'Spontaneous - adequate'
        , 'Spontaneous - tenuous'
        , 'Ventilated - chronic dependency'
        , 'Ventilated - with daily extubation evaluation'
        , 'Ventilated - with no daily extubation trial'
    )
)

, ventaliation_info_time_00 as (
    select patientunitstayid, cplitemoffset
    , hospitaldischargeoffset
    , case when vent_type = 1 then 'invasive' 
    when vent_type = 0 then 'non-invasive'
    else null end as vent_type 
    , activeupondischarge, vent_start_flag
    from (
        select patientunitstayid, cplitemoffset, hospitaldischargeoffset
        , max(case when vent_type = 'invasive' then 1 
                when vent_type = 'non-invasive' then 0 
                else null end
                ) as vent_type
        , max(case when activeupondischarge is true then 1 else 0 end) as activeupondischarge
        , max(case when vent_start_flag = 1 then 1 else 0 end) as vent_start_flag
        from ventaliation_info_initial
        group by patientunitstayid, cplitemoffset, hospitaldischargeoffset
    )
)

-- drop: the first row with 'Spontaneous - adequate' and activeupondischarge = 'False'
-- later existing ventilation records
, ventaliation_info_time_01 as (
    select patientunitstayid, hospitaldischargeoffset, cplitemoffset, vent_type, vent_start_flag, activeupondischarge
    from (
        select *, case when rn = 1 and vent_start_flag = 0 and activeupondischarge = 0 then 1 else 0 end as drop_flag
        from (
            select *, ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY cplitemoffset) as rn
            from ventaliation_info_time_00
        )
    )
    where drop_flag = 0
)

-- drop patients: the first row with 'Spontaneous - adequate' and activeupondischarge = 'True' and no existing 'invasive'
-- we thought they didn't receive ventilation
, drop_info_part_0 as (
    select v1.*, ROW_NUMBER() OVER (PARTITION BY v1.patientunitstayid ORDER BY cplitemoffset) as rn
    from ventaliation_info_time_01 v1
    inner join (
        select patientunitstayid, max(case when vent_type = 'invasive' then 1 else 0 end) as flag
        from ventaliation_info_time_01
        group by patientunitstayid
    ) v2
    on v1.patientunitstayid = v2.patientunitstayid
    and v2.flag = 0
)

, drop_info_part as (
    select distinct patientunitstayid
    from drop_info_part_0
    where rn = 1 and activeupondischarge = 1
)

, ventaliation_info_time_0 as (
    select patientunitstayid, hospitaldischargeoffset, cplitemoffset, vent_type, vent_start_flag, activeupondischarge
    from ventaliation_info_time_01
    where patientunitstayid not in (
        select patientunitstayid from drop_info_part
    )
)

select *
from ventaliation_info_time_0;


drop table if exists `db_name.ventaliation_info_use_4_eicu`;
create table `db_name.ventaliation_info_use_4_eicu` as

with ventaliation_info_initial as (
    select ce.patientunitstayid
	, activeupondischarge
    , hospitaldischargeoffset
	, cplitemoffset
	, cplgroup
	, cplitemvalue
	, case 
	when cplitemvalue in (
        'Intubated/nasal ETT'
        , 'Intubated/oral ETT'
        , 'Intubated/trach-acute'
        , 'Intubated/trach-chronic'
        , 'Ventilated - chronic dependency'
        , 'Ventilated - with daily extubation evaluation'
        , 'Ventilated - with no daily extubation trial'                
    ) then 'invasive'
    when cplitemvalue in (
        'Not intubated/normal airway'
        , 'Not intubated/partial airway obstruction'
        , 'Non-invasive ventilation'
        , 'Spontaneous - tenuous'
    ) then 'non-invasive' else null end as vent_type
    , case
    when cplitemvalue = 'Spontaneous - adequate' then 0 else 1 end as vent_start_flag   
    from `physionet-data.eicu_crd.careplangeneral` ce 
    inner join `physionet-data.eicu_crd_derived.icustay_detail` icud 
    on ce.patientunitstayid = icud.patientunitstayid
    and ce.cplitemoffset <= icud.hospitaldischargeoffset -- we drop the info out of the same icu admitted
	where cplgroup in ('Airway', 'Ventilation') 
	and cplitemvalue in (
        'Intubated/nasal ETT'
        , 'Intubated/oral ETT'
        , 'Intubated/trach-acute'
        , 'Intubated/trach-chronic'
        , 'Not intubated/normal airway'
        , 'Not intubated/partial airway obstruction'
        , 'Non-invasive ventilation'
        , 'Spontaneous - adequate'
        , 'Spontaneous - tenuous'
        , 'Ventilated - chronic dependency'
        , 'Ventilated - with daily extubation evaluation'
        , 'Ventilated - with no daily extubation trial'
    )
)

, ventaliation_info_time_0 as (
    select *
    from `db_name.ventaliation_info_time_0_eicu`
)

-- identify the 'non-invasive' dischargestatus true, while existing 'invasive' type
-- which should be changed to end before the 'invasive' start
, change_info_part as (
    select v1.patientunitstayid, v1.cplitemoffset
    from (
        select *
        from ventaliation_info_time_0
        where vent_type = 'non-invasive'
        and activeupondischarge = 1
    ) v1
    inner join (
        select *
        from ventaliation_info_time_0
        where vent_type = 'invasive'
    ) v2
    on v1.patientunitstayid = v2.patientunitstayid
    and v1.cplitemoffset < v2.cplitemoffset
    where v1.patientunitstayid in (
        select distinct patientunitstayid 
        from ventaliation_info_initial
        where vent_type = 'non-invasive'
        and activeupondischarge is true
        and patientunitstayid in (
            select distinct patientunitstayid
            from ventaliation_info_initial
            where vent_type = 'invasive' 
        )
    )
)

, ventaliation_info_time_1 as (
    select v0.patientunitstayid, v0.cplitemoffset
    , v0.hospitaldischargeoffset
    , case 
    when v0.patientunitstayid = ci.patientunitstayid and v0.cplitemoffset = ci.cplitemoffset then 0
    else v0.activeupondischarge end as activeupondischarge 
    , vent_type, vent_start_flag
    from ventaliation_info_time_0 v0 
    left join change_info_part ci 
    on v0.patientunitstayid = ci.patientunitstayid
    and v0.cplitemoffset = ci.cplitemoffset
)

, ventaliation_info_time as (
    select *
    from (
        select patientunitstayid, cplitemoffset, vent_type, vent_start_flag
        from ventaliation_info_time_1
        union all
        select patientunitstayid, hospitaldischargeoffset as cplitemoffset, vent_type, 0 as vent_start_flag
        from ventaliation_info_time_1
        where activeupondischarge = 1
        union all -- the last record was vent with false status, since we didn't know the end time, we set adding 60min as endtime
        select patientunitstayid, (cplitemoffset + 60) as cplitemoffset, vent_type, 0 as vent_start_flag
        from (
            select *
            from (
                select *, ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY cplitemoffset desc) as rn
                from ventaliation_info_time_1
            )
            where rn = 1 and activeupondischarge = 0 and vent_start_flag = 1
        )
    )
    order by patientunitstayid, cplitemoffset
)

-- user_id, key, sort
-- patientid, vent_start_flag, cplitoffset
-- https://www.postgresql.org/message-id/20130204103454.0b3c6b23@tucholsky.experteer.muc
-- https://www.postgresql.org/message-id/CAGnEbohhKmW55oB0FpQd3naXBkwi74E%3D8DRZBGFjS-MeGpXLaA%40mail.gmail.com
-- https://stackoverflow.com/questions/10614505/window-functions-and-more-local-aggregation/10624628#10624628

, ventaliation_info_use_0 AS (
    SELECT patientunitstayid, vent_start_flag, cplitemoffset
    , CASE 
    WHEN lag(vent_start_flag) OVER (PARTITION BY patientunitstayid ORDER BY patientunitstayid, cplitemoffset) = vent_start_flag 
    THEN NULL ELSE 1 END r
    FROM ventaliation_info_time
)

,  ventaliation_info_use_1 AS (
    SELECT patientunitstayid, vent_start_flag, cplitemoffset, r
    , sum(r) OVER (ORDER BY patientunitstayid, cplitemoffset) grp
    FROM ventaliation_info_use_0
)

, ventaliation_info_use_2 as (
    SELECT min(patientunitstayid) as patientunitstayid, min(vent_start_flag) as vent_start_flag,
    min(cplitemoffset) as sort_first,
    max(cplitemoffset) as sort_last
    FROM ventaliation_info_use_1
    GROUP BY grp
)

-- get the start and end time of each patient
, ventaliation_info_use_3 as (
    select patientunitstayid, vent_start_flag
    , case when vent_start_flag = 1 then sort_first
    when vent_start_flag = 0 then sort_last
    end as cplitemoffset
    from ventaliation_info_use_2
)

-- here we check the abnormal types:
-- non-invasive cplitemoffset = hospitaldischargeoffset
-- only existing 'Spontaneous - adequate' records
, ventaliation_info_use_4 as (
    select *
    from ventaliation_info_use_3
    where patientunitstayid not in (
        select distinct patientunitstayid
        from (
            select patientunitstayid, sum(vent_start_flag) as num
            from ventaliation_info_use_3
            group by patientunitstayid
        )
        where num = 0
    )
)

select * -- patientunitstayid, cplitemoffset as starttime
from ventaliation_info_use_4
order by patientunitstayid, cplitemoffset;



drop table if exists `db_name.pivoted_vent_eicu`;
create table `db_name.pivoted_vent_eicu` as

-- can't cover 4 patients with special types, we manually set them by checking initial info
-- patientunitstayid in (1565479, 1571346, 1571446, 1589211)

with ventaliation_info_use_4 as (
    select *
    from `db_name.ventaliation_info_use_4_eicu`
)

, pivoted_vent_eicu_0 as (
    select *
    from ventaliation_info_use_4
    where patientunitstayid not in (1565479, 1571346, 1571446, 1589211)
    union all
    select patientunitstayid
    , 1 as vent_start_flag
    , case when patientunitstayid = 1571346 then 2091
    when patientunitstayid = 1571446 then 1430
    when patientunitstayid = 1589211 then 2372
    else null end as cplitemoffset
    from ventaliation_info_use_4
    where patientunitstayid in (1571346, 1571446, 1589211)  -- drop 1565479
    union all
    select patientunitstayid
    , 0 as vent_start_flag
    , case when patientunitstayid = 1571346 then 5170
    when patientunitstayid = 1571446 then (1430+60)
    when patientunitstayid = 1589211 then 4903
    else null end as cplitemoffset
    from ventaliation_info_use_4
    where patientunitstayid in (1571346, 1571446, 1589211)  -- drop 1565479
) 

, pivoted_vent_eicu_1 as (
    select patientunitstayid, vent_start_flag, cplitemoffset
    from pivoted_vent_eicu_0
    order by patientunitstayid, cplitemoffset, vent_start_flag
)

, pivoted_vent_eicu_s as (
    select patientunitstayid, cplitemoffset as starttime
    , ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY cplitemoffset desc) as rn
    from pivoted_vent_eicu_0
    where vent_start_flag = 1
)

, pivoted_vent_eicu_e as (
    select patientunitstayid, cplitemoffset as endtime
    , ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY cplitemoffset desc) as rn
    from pivoted_vent_eicu_1
    where vent_start_flag = 0
)

select ps.patientunitstayid, ps.starttime, pe.endtime
from pivoted_vent_eicu_s ps 
inner join pivoted_vent_eicu_e pe 
on ps.patientunitstayid = pe.patientunitstayid
and ps.rn = pe.rn
order by ps.patientunitstayid, ps.starttime;



drop table if exists `db_name.ventaliation_info_time_0_eicu`;
drop table if exists `db_name.ventaliation_info_use_4_eicu`;
*/