CREATE OR REPLACE PACKAGE BIHR.REP_HEADCOUNT_MOVING_PKG IS

  /*------------------------------------------------------------------------------
    $Id$
    Название: Пакет  формирования REP_HEADCOUNT_MOVING_T
    Описание:
  ------------------------------------------------------------------------------*/
  TYPE tbl IS TABLE OF REP_HEADCOUNT_MOVING_T%ROWTYPE;

  procedure update_tbl(V_period_name varchar2,debug boolean:=false);

  PROCEDURE refresh(V_period_name varchar2);

  PROCEDURE TRUNCATE_PARTITION(V_period_name varchar2);

  PROCEDURE insert_tbl(V_period_name varchar2);

  PROCEDURE refresh;

END;
/
CREATE OR REPLACE PACKAGE BODY BIHR.REP_HEADCOUNT_MOVING_PKG IS

/*------------------------------------------------------------------------------
  $Id$
  Название: Пакет  формирования REP_HEADCOUNT_MOVING_T
  Описание:
------------------------------------------------------------------------------*/
  function exists_partition(V_period_name varchar2) return number as
    v_count     number;
    v_partition varchar2(10) := 'p_' || replace(V_period_name, '-', '');
  begin
    SELECT COUNT(*)
    INTO v_count
    FROM all_tab_partitions
    WHERE table_name = 'REP_HEADCOUNT_MOVING_T' AND table_owner = 'BIHR' AND
          partition_name = upper(v_partition);
    return v_count;
  end;

  PROCEDURE set_partition(V_period_name varchar2) is
    v_partition varchar2(10) := 'p_' || replace(V_period_name, '-', '');
  begin
    IF exists_partition(V_period_name) = 0 THEN
/*      EXECUTE IMMEDIATE*/
dbms_output.put_line( 'ALTER TABLE BIHR.REP_HEADCOUNT_MOVING_T ADD PARTITION  ' ||
                        v_partition || ' VALUES (''' || V_period_name || ''') ');
       EXECUTE IMMEDIATE  'ALTER TABLE BIHR.REP_HEADCOUNT_MOVING_T ADD PARTITION  ' ||
                        v_partition || ' VALUES (''' || V_period_name || ''') ';
    END IF;
  end;

  PROCEDURE TRUNCATE_PARTITION(V_period_name varchar2) is
  begin
    EXECUTE IMMEDIATE 'ALTER TABLE BIHR.REP_HEADCOUNT_MOVING_T TRUNCATE PARTITION   p_' ||
                      replace(v_period_name, '-', '');
  EXCEPTION
    WHEN OTHERS THEN
      null;
  end;

PROCEDURE CREATE_STAT(V_period_name varchar2) is
  v_partition varchar2(10) := 'p_' || replace(V_period_name, '-', '');
begin
  dbms_stats.gather_table_stats(ownname     => 'BIHR',
                                tabname     => 'REP_HEADCOUNT_MOVING_T',
                                partname    => v_partition,
                                granularity => 'PARTITION');
end;

PROCEDURE refresh is
  cursor c_period is
    with periods as
     (select period_name
            ,start_date
            ,last_update_date
            ,closing_status
      from (select period_name
                  ,start_date
                  ,last_update_date
                  ,closing_status
                  ,row_number() over(partition by period_name order by last_update_date desc) rn
            from bimatview.GL_PERIOD_STATUSES t
            where application_id IN (801, 101) and
                  ADJUSTMENT_PERIOD_FLAG = 'N' and
                  start_date >= to_date(' 01.01.2012', 'dd.mm.yyyy') and
                  end_date < sysdate and closing_status in ('O', 'C'))
      where rn = 1),
    periods_change as
     (select period_name
      from periods t
      where closing_status = 'O' or
            (closing_status = 'C' and not  exists
             (select 1
              from BIHR.REP_HEADCOUNT_MOVING_INSERT_T
              where period_name = t.period_name and
                    insert_date > last_update_date))),
    lead_periods as
     (select p.period_name
            ,lead(p.period_name) over(order by p.start_date) lead_period_name
      from periods p)
    select period_name
    from periods
    where period_name in (select lead_period_name
                          from lead_periods p
                              ,periods_change ch
                          where ch.period_name = p.period_name and
                                lead_period_name is not null
                          union all
                          select period_name
                          from periods_change)
    order by start_date;
begin
  for i in c_period
  loop
    refresh(i.period_name);
  end loop;
end;

  PROCEDURE refresh(V_period_name varchar2) is
  begin
     DBMS_APPLICATION_INFO.SET_ACTION(
      action_name => V_period_name);
    delete from BIHR.REP_HEADCOUNT_MOVING_INSERT_T where period_name =V_period_name;
    insert into  BIHR.REP_HEADCOUNT_MOVING_INSERT_T values(V_period_name,sysdate);
  commit;
    TRUNCATE_PARTITION(V_period_name);
    insert_tbl(V_period_name);
    update_tbl(V_period_name);
  end;

  PROCEDURE insert_tbl(V_period_name varchar2) is
  begin
    set_partition(v_period_name);
    BICOMMON_API_PKG.init_parameters('REP_HEADCOUNT_MOVING', v_period_name);
    INSERT INTO bihr.REP_HEADCOUNT_MOVING_T
      select * from BIHR.REP_HEADCOUNT_INFO_V;
    commit;
    CREATE_STAT(V_period_name);
  end;

  procedure update_tbl(V_period_name varchar2,debug boolean:=false) as
    cursor c_period_lag(v_period_name varchar2) is
      select period_name
      from (select distinct period_name
                           ,start_date
            from bimatview.GL_PERIOD_STATUSES
            where start_date <
                  (select distinct start_date
                   from bimatview.GL_PERIOD_STATUSES t
                   where application_id IN (801, 101) and
                         period_name = v_period_name
                          ) and application_id IN (801, 101) and ADJUSTMENT_PERIOD_FLAG='N'
            ORDER BY start_date DESC);
    cursor c_month(v_period_name varchar2) is
      select mth_id
            ,mth_desc_us
            ,mth_end_dt
             ,mth_start_dt
      from bihr.OEBS_GL_PERIOD_STATUSES
          ,bihr.lu_dt_month
      where mth_start_dt = start_date and period_name = v_period_name
      group by mth_id
              ,mth_desc_us
              ,mth_end_dt
              ,mth_start_dt;
    v_lag_period    varchar2(10);
    v_month         c_month%rowtype;
    v_partition     varchar2(10) := upper('p_' ||
                                          replace(V_period_name, '-', ''));
    v_partition_lag varchar2(10);
    v_sql           varchar2(4000);
  begin
    v_lag_period := '';
    open c_period_lag(V_period_name);
    fetch c_period_lag
      into v_lag_period;
    close c_period_lag;
    v_partition_lag := upper('p_' || replace(v_lag_period, '-', ''));
    open c_month(V_period_name);
    fetch c_month
      into v_month;
    close c_month;
    if v_lag_period is not null and exists_partition(v_lag_period) > 0 then
      --найм
      v_sql := ' update bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')  t set t.cause =''найм'',  t.gl_actual_headcount_delta = t.ACTUAL_HEADCOUNT
     where not exists (select 1 from bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag where
     t_lag.PARTY_ID = t.PARTY_ID) and  t.assignment_id  is not null';
     if debug then
      dbms_output.put_line('найм');
       dbms_output.put_line(v_sql);
      else
       EXECUTE IMMEDIATE v_sql;
      end if;
      --увольнение
      v_sql := ' insert into  bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')  t_lag
     select  :mth_id, :mth_desc_us, :mth_end_dt, t_lag.party_id, t_lag.login, t_lag.full_name,  :mth_start_dt, t_lag.assignment_id, t_lag.position_name, t_lag.position_num, t_lag.bp_status_code, t_lag.payment_type_name, t_lag.year_, t_lag.location_name_ru, t_lag.position_grade_level, t_lag.profession, t_lag.assign_job_name, t_lag.assign_pay_basis_name, t_lag.assign_staff_rate, t_lag.product_value, t_lag.product_name, t_lag.leaf_dep_name, t_lag.dep_name_l2, t_lag.department_concat_hier, t_lag.segment1, t_lag.segment1_name, t_lag.segment2, t_lag.segment2_name, t_lag.segment3, t_lag.segment3_name, t_lag.segment4, t_lag.segment4_name, t_lag.segment5, t_lag.segment5_name, t_lag.segment6, t_lag.segment6_name, t_lag.segment7, t_lag.segment7_name, t_lag.segment8, t_lag.segment8_name, t_lag.segment9, t_lag.segment9_name, t_lag.segment10, t_lag.segment10_name, t_lag.segment11, t_lag.segment11_name, t_lag.segment12, t_lag.segment12_name, 0, t_lag.bo_concat_hier, t_lag.bp_list, t_lag.bo_id, t_lag.level10_name, t_lag.level9_name, t_lag.level8_name, t_lag.level7_name, t_lag.level6_name, t_lag.level5_name, t_lag.level4_name, t_lag.level3_name, t_lag.level2_name, t_lag.level1_name, t_lag.bo_concat_hier_altern, t_lag.bo_id_altern, t_lag.level10_name_altern, t_lag.level9_name_altern, t_lag.level8_name_altern, t_lag.level7_name_altern, t_lag.level6_name_altern, t_lag.level5_name_altern, t_lag.level4_name_altern, t_lag.level3_name_altern, t_lag.level2_name_altern, t_lag.level1_name_altern, t_lag.data_type, t_lag.is_maternity, t_lag.is_intern, t_lag.is_outstaff, t_lag.status, t_lag.position_id, t_lag.payment_type, t_lag.ch_assignment_status, t_lag.num_info1, t_lag.num_info9, t_lag.num_info10, t_lag.num_info2, t_lag.insert_date, :V_period_name, ''увольнение'', -t_lag.ACTUAL_HEADCOUNT, t_lag.REPLACEMENT,t_lag.concat_key
     from   bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag
                 where
     not exists ( select 1 from  bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')  t  where  t_lag.PARTY_ID = t.PARTY_ID)
      and t_lag.ACTUAL_HEADCOUNT > 0  and  t_lag.assignment_id  is not null';
      if debug then
       dbms_output.put_line('увольнение');
       dbms_output.put_line(v_sql);
      else
      EXECUTE IMMEDIATE v_sql  using v_month.mth_id, v_month.mth_desc_us, v_month.mth_end_dt, v_month.mth_start_dt, V_period_name;
      end if;
      --изменения по ручным загрузкам
      v_sql := ' insert into  bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition || ')
     select  :mth_id, :mth_desc_us, :mth_end_dt, t_lag.party_id, t_lag.login, t_lag.full_name, :mth_start_dt, t_lag.assignment_id, t_lag.position_name, t_lag.position_num, t_lag.bp_status_code, t_lag.payment_type_name, t_lag.year_, t_lag.location_name_ru, t_lag.position_grade_level, t_lag.profession, t_lag.assign_job_name, t_lag.assign_pay_basis_name, t_lag.assign_staff_rate, t_lag.product_value, t_lag.product_name, t_lag.leaf_dep_name, t_lag.dep_name_l2, t_lag.department_concat_hier, t_lag.segment1, t_lag.segment1_name, t_lag.segment2, t_lag.segment2_name, t_lag.segment3, t_lag.segment3_name, t_lag.segment4, t_lag.segment4_name, t_lag.segment5, t_lag.segment5_name, t_lag.segment6, t_lag.segment6_name, t_lag.segment7, t_lag.segment7_name, t_lag.segment8, t_lag.segment8_name, t_lag.segment9, t_lag.segment9_name, t_lag.segment10, t_lag.segment10_name, t_lag.segment11, t_lag.segment11_name, t_lag.segment12, t_lag.segment12_name, 0, t_lag.bo_concat_hier, t_lag.bp_list, t_lag.bo_id, t_lag.level10_name, t_lag.level9_name, t_lag.level8_name, t_lag.level7_name, t_lag.level6_name, t_lag.level5_name, t_lag.level4_name, t_lag.level3_name, t_lag.level2_name, t_lag.level1_name, t_lag.bo_concat_hier_altern, t_lag.bo_id_altern, t_lag.level10_name_altern, t_lag.level9_name_altern, t_lag.level8_name_altern, t_lag.level7_name_altern, t_lag.level6_name_altern, t_lag.level5_name_altern, t_lag.level4_name_altern, t_lag.level3_name_altern, t_lag.level2_name_altern, t_lag.level1_name_altern, t_lag.data_type, t_lag.is_maternity, t_lag.is_intern, t_lag.is_outstaff, t_lag.status, t_lag.position_id, t_lag.payment_type, t_lag.ch_assignment_status, t_lag.num_info1, t_lag.num_info9, t_lag.num_info10, t_lag.num_info2, t_lag.insert_date, :V_period_name, ''изменения по ручным загрузкам'', -t.ACTUAL_HEADCOUNT, t_lag.REPLACEMENT,t_lag.concat_key
     from   bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag ,
               ( select  concat_key,sum(ACTUAL_HEADCOUNT) ACTUAL_HEADCOUNT   from   bihr.REP_HEADCOUNT_MOVING_T   partition(  ' ||
               v_partition_lag ||')   where
                    assignment_id  is null  group by concat_key having  sum(ACTUAL_HEADCOUNT)<>0)  t  where t_lag.assignment_id  is null 
                    and t.concat_key=t_lag.concat_key ';

      if debug then
        dbms_output.put_line('списание');
       dbms_output.put_line(v_sql);
      else
      EXECUTE IMMEDIATE v_sql  using v_month.mth_id, v_month.mth_desc_us, v_month.mth_end_dt, v_month.mth_start_dt, V_period_name;
      end if;
      --стажеры/декрет/Outstaff/изменение численности
      v_sql := ' update bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')  t set
      (t.cause , t.gl_actual_headcount_delta) =
      (select case
      when  t.is_intern = 1 and  t_lag.is_intern= 0 then
      ''увольнение в стажеры''
       when  t.is_intern= 0 and  t_lag.is_intern>= 1 then
       ''найм стажера''
       when  t.is_maternity>= 1 and  t_lag.is_maternity = 0 then
       ''выход из декрета''
       when  t.is_maternity = 0 and  t_lag.is_maternity>= 1 then
       ''уход в декрет''
       when  t.is_Outstaff= 0 and  t_lag.is_Outstaff>= 1 then
       ''увольнение в Outstaff''
       when  t.is_Outstaff>= 1 and t_lag.is_Outstaff= 0 then
       ''найм из Outstaff''
       end
       ,t.ACTUAL_HEADCOUNT - t_lag.ACTUAL_HEADCOUNT
      from bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag where
     t_lag.PARTY_ID = t.PARTY_ID and ((t.is_intern-t_lag.is_intern)+(t.is_maternity-t_lag.is_maternity)+(t.is_Outstaff-t_lag.is_Outstaff))<>0
      and  t.assignment_id   =t_lag.assignment_id  and t.ACTUAL_HEADCOUNT - t_lag.ACTUAL_HEADCOUNT <> 0 and rownum=1)
     where  exists (select 1 from bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag where
     t_lag.PARTY_ID = t.PARTY_ID and ((t.is_intern-t_lag.is_intern)+(t.is_maternity-t_lag.is_maternity)+(t.is_Outstaff-t_lag.is_Outstaff))<>0
     and  t.assignment_id   =t_lag.assignment_id  and t.ACTUAL_HEADCOUNT - t_lag.ACTUAL_HEADCOUNT <> 0) ';

       if debug then
       dbms_output.put_line('стажеры/декрет/Outstaff/изменение численности');
       dbms_output.put_line(v_sql);
      else
       EXECUTE IMMEDIATE v_sql;
      end if;

            --изменение численности
      v_sql := ' update bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')  t set
      (t.cause , t.gl_actual_headcount_delta) =
      (select
       ''изменение численности''
       ,t.ACTUAL_HEADCOUNT - t_lag.ACTUAL_HEADCOUNT
      from bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag where
     t_lag.PARTY_ID = t.PARTY_ID and nvl(t.concat_key,'' '') = nvl(t_lag.concat_key,'' '')
      and  t.assignment_id   =t_lag.assignment_id  and t.ACTUAL_HEADCOUNT - t_lag.ACTUAL_HEADCOUNT <> 0 and rownum=1)
     where  t.gl_actual_headcount_delta is null and exists (select 1 from bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag where
     t_lag.PARTY_ID = t.PARTY_ID and nvl(t.concat_key,'' '') = nvl(t_lag.concat_key,'' '')
     and  t.assignment_id   =t_lag.assignment_id  and t.ACTUAL_HEADCOUNT - t_lag.ACTUAL_HEADCOUNT <> 0  ) ';
       if debug then
       dbms_output.put_line('изменение численности');
       dbms_output.put_line(v_sql);
      else
       EXECUTE IMMEDIATE v_sql;
      end if;

       ---------------------переводы ------------------------------------------------------
      --1. найти переводы

      --  'перевод из c бюджетом'
            v_sql := ' insert into  bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')
     select  :mth_id, :mth_desc_us, :mth_end_dt, t_lag.party_id, t_lag.login, t_lag.full_name, :mth_start_dt,  t_lag.assignment_id, t_lag.position_name, t_lag.position_num, t_lag.bp_status_code, t_lag.payment_type_name, t_lag.year_, t_lag.location_name_ru, t_lag.position_grade_level, t_lag.profession, t_lag.assign_job_name, t_lag.assign_pay_basis_name, t_lag.assign_staff_rate, t_lag.product_value, t_lag.product_name, t_lag.leaf_dep_name, t_lag.dep_name_l2, t_lag.department_concat_hier, t_lag.segment1, t_lag.segment1_name, t_lag.segment2, t_lag.segment2_name, t_lag.segment3, t_lag.segment3_name, t_lag.segment4, t_lag.segment4_name, t_lag.segment5, t_lag.segment5_name, t_lag.segment6, t_lag.segment6_name, t_lag.segment7, t_lag.segment7_name, t_lag.segment8, t_lag.segment8_name, t_lag.segment9, t_lag.segment9_name, t_lag.segment10, t_lag.segment10_name, t_lag.segment11, t_lag.segment11_name, t_lag.segment12, t_lag.segment12_name,0, t_lag.bo_concat_hier, t_lag.bp_list, t_lag.bo_id, t_lag.level10_name, t_lag.level9_name, t_lag.level8_name, t_lag.level7_name, t_lag.level6_name, t_lag.level5_name, t_lag.level4_name, t_lag.level3_name, t_lag.level2_name, t_lag.level1_name, t_lag.bo_concat_hier_altern, t_lag.bo_id_altern, t_lag.level10_name_altern, t_lag.level9_name_altern, t_lag.level8_name_altern, t_lag.level7_name_altern, t_lag.level6_name_altern, t_lag.level5_name_altern, t_lag.level4_name_altern, t_lag.level3_name_altern, t_lag.level2_name_altern, t_lag.level1_name_altern, t_lag.data_type, t_lag.is_maternity, t_lag.is_intern, t_lag.is_outstaff, t_lag.status, t_lag.position_id, t_lag.payment_type, t_lag.ch_assignment_status, t_lag.num_info1, t_lag.num_info9, t_lag.num_info10, t_lag.num_info2, t_lag.insert_date, :V_period_name, ''перевод из с бюджетом'', -t_lag.ACTUAL_HEADCOUNT, t_lag.REPLACEMENT,t_lag.concat_key
     from   bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag  where t_lag.assignment_id  is not null and  nvl(t_lag.bp_list,''0'')<>''0'' and   ACTUAL_HEADCOUNT >0 and
               exists
               (
                select 1  from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition ||
               ')  where party_id=t_lag.party_id  and bp_list= t_lag.bp_list and  ACTUAL_HEADCOUNT >0
               )
              and
               not exists
               (
                select 1  from bihr.REP_HEADCOUNT_MOVING_T partition( ' || v_partition ||  ')  where party_id=t_lag.party_id   and  concat_key=t_lag.concat_key 
               )
               and
               concat_key in
               (
                select concat_key  from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition_lag ||
               ')  where party_id=t_lag.party_id and  ACTUAL_HEADCOUNT >0
               minus
                 select concat_key  from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition ||
               ')  where party_id=t_lag.party_id and  ACTUAL_HEADCOUNT >0)';

     if debug then
        dbms_output.put_line('перевод из c бюджетом');
       dbms_output.put_line(v_sql);
      else
      EXECUTE IMMEDIATE v_sql  using v_month.mth_id, v_month.mth_desc_us, v_month.mth_end_dt, v_month.mth_start_dt, V_period_name;
      end if;
       --  'перевод из без бюджета'
       v_sql := ' insert into  bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')
     select  :mth_id, :mth_desc_us, :mth_end_dt, t_lag.party_id, t_lag.login, t_lag.full_name, :mth_start_dt,  t_lag.assignment_id, t_lag.position_name, t_lag.position_num, t_lag.bp_status_code, t_lag.payment_type_name, t_lag.year_, t_lag.location_name_ru, t_lag.position_grade_level, t_lag.profession, t_lag.assign_job_name, t_lag.assign_pay_basis_name, t_lag.assign_staff_rate, t_lag.product_value, t_lag.product_name, t_lag.leaf_dep_name, t_lag.dep_name_l2, t_lag.department_concat_hier, t_lag.segment1, t_lag.segment1_name, t_lag.segment2, t_lag.segment2_name, t_lag.segment3, t_lag.segment3_name, t_lag.segment4, t_lag.segment4_name, t_lag.segment5, t_lag.segment5_name, t_lag.segment6, t_lag.segment6_name, t_lag.segment7, t_lag.segment7_name, t_lag.segment8, t_lag.segment8_name, t_lag.segment9, t_lag.segment9_name, t_lag.segment10, t_lag.segment10_name, t_lag.segment11, t_lag.segment11_name, t_lag.segment12, t_lag.segment12_name,0, t_lag.bo_concat_hier, t_lag.bp_list, t_lag.bo_id, t_lag.level10_name, t_lag.level9_name, t_lag.level8_name, t_lag.level7_name, t_lag.level6_name, t_lag.level5_name, t_lag.level4_name, t_lag.level3_name, t_lag.level2_name, t_lag.level1_name, t_lag.bo_concat_hier_altern, t_lag.bo_id_altern, t_lag.level10_name_altern, t_lag.level9_name_altern, t_lag.level8_name_altern, t_lag.level7_name_altern, t_lag.level6_name_altern, t_lag.level5_name_altern, t_lag.level4_name_altern, t_lag.level3_name_altern, t_lag.level2_name_altern, t_lag.level1_name_altern, t_lag.data_type, t_lag.is_maternity, t_lag.is_intern, t_lag.is_outstaff, t_lag.status, t_lag.position_id, t_lag.payment_type, t_lag.ch_assignment_status, t_lag.num_info1, t_lag.num_info9, t_lag.num_info10, t_lag.num_info2, t_lag.insert_date, :V_period_name, ''перевод из без бюджета'', -t_lag.ACTUAL_HEADCOUNT, t_lag.REPLACEMENT,t_lag.concat_key
     from   bihr.REP_HEADCOUNT_MOVING_T   partition( ' ||
               v_partition_lag ||
               ') t_lag  where t_lag.assignment_id  is not null and   ACTUAL_HEADCOUNT >0 and
               exists
               (
                select 1  from bihr.REP_HEADCOUNT_MOVING_T partition( ' || v_partition ||  ')  where party_id=t_lag.party_id   and  ACTUAL_HEADCOUNT >0
               )
               and
               not exists
               (
                select 1  from bihr.REP_HEADCOUNT_MOVING_T partition( ' || v_partition ||  ')  where party_id=t_lag.party_id   and  concat_key=t_lag.concat_key 
               )
               and
               concat_key in
               (
                select concat_key  from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition_lag ||
               ')  where party_id=t_lag.party_id  and  ACTUAL_HEADCOUNT >0
               minus
                 select concat_key  from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition ||
               ')  where party_id=t_lag.party_id and  ACTUAL_HEADCOUNT >0)';

     if debug then
        dbms_output.put_line('перевод из без бюджетом');
       dbms_output.put_line(v_sql);
      else
      EXECUTE IMMEDIATE v_sql  using v_month.mth_id, v_month.mth_desc_us, v_month.mth_end_dt,v_month.mth_start_dt,  V_period_name;
      end if;

          --'перевод в с бюджетом'
     v_sql := ' update bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')  t set
      t.cause='' перевод в с бюджетом'', t.gl_actual_headcount_delta=t.ACTUAL_HEADCOUNT
      where  t.assignment_id  is not null and  nvl(t.bp_list,''0'')<>''0'' and gl_actual_headcount_delta is null and
       concat_key in
               (
                select concat_key from  bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition ||
               ')  where party_id=t.party_id  and  ACTUAL_HEADCOUNT >0
               minus
                 select concat_key from  bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition_lag ||
               ')  where party_id=t.party_id and  ACTUAL_HEADCOUNT >0)
               and
                exists
               (
                select 1 from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition_lag ||
               ')  where party_id=t.party_id  and bp_list= t.bp_list and  ACTUAL_HEADCOUNT >0
               ) ';

     if debug then
        dbms_output.put_line('перевод в с бюджетом');
       dbms_output.put_line(v_sql);
      else
      EXECUTE IMMEDIATE v_sql  ;
      end if;
                    --'перевод в без  бюджета'
     v_sql := ' update bihr.REP_HEADCOUNT_MOVING_T partition(  ' ||
               v_partition ||
               ')  t set
      t.cause='' перевод в без  бюджета'', t.gl_actual_headcount_delta=t.ACTUAL_HEADCOUNT
      where  t.assignment_id  is not null and gl_actual_headcount_delta is null and
       concat_key in
               (
                select concat_key from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition ||
               ')  where party_id=t.party_id and  ACTUAL_HEADCOUNT >0
               minus
                 select concat_key from bihr.REP_HEADCOUNT_MOVING_T partition( ' ||
               v_partition_lag ||
               ')  where party_id=t.party_id and  ACTUAL_HEADCOUNT >0 ) ';

     if debug then
        dbms_output.put_line('перевод в без  бюджета');
       dbms_output.put_line(v_sql);
      else
      EXECUTE IMMEDIATE v_sql  ;
      end if;

    end if;

      --replacement
      v_sql := '  update bihr.REP_HEADCOUNT_MOVING_T partition(   ' ||
               v_partition ||
               ')  t set
                 gl_actual_headcount_delta =nvl(gl_actual_headcount_delta,0),
               t.cause=trim ( t.cause ) || case when replacement <> 0 and trim ( t.cause ) is not null  then  '' замена''  else '' новый'' end  ';
      if debug then
       dbms_output.put_line('replacement');
       dbms_output.put_line(v_sql);
      else
       EXECUTE IMMEDIATE v_sql;
      end if;
      commit;

  end;




END;
/
