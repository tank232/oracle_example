CREATE OR REPLACE PACKAGE BODY BIHR.rep_headcount_moving_pkg IS

    /*------------------------------------------------------------------------------
      $Id$
      Название: Пакет  формирования REP_HEADCOUNT_MOVING_T
      Описание:
    ------------------------------------------------------------------------------*/
    FUNCTION exists_partition(v_period_name VARCHAR2) RETURN NUMBER AS
        v_count     NUMBER;
        v_partition VARCHAR2(10) := 'p_' || REPLACE(v_period_name, '-', '');
    BEGIN
        SELECT count(*)
        INTO v_count
        FROM all_tab_partitions
        WHERE table_name = 'REP_HEADCOUNT_MOVING_T' AND
              table_owner = 'BIHR' AND partition_name = upper(v_partition);
        RETURN v_count;
    END;

    PROCEDURE set_partition(v_period_name VARCHAR2) IS
        v_partition VARCHAR2(10) := 'p_' || REPLACE(v_period_name, '-', '');
    BEGIN
        IF exists_partition(v_period_name) = 0 THEN
            EXECUTE IMMEDIATE 'ALTER TABLE BIHR.REP_HEADCOUNT_MOVING_T ADD PARTITION  ' ||
                              v_partition || ' VALUES (''' || v_period_name ||
                              ''') ';
        END IF;
    END;

    PROCEDURE truncate_partition(v_period_name VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE BIHR.REP_HEADCOUNT_MOVING_T TRUNCATE PARTITION   p_' ||
                          REPLACE(v_period_name, '-', '');
    exception
        WHEN others THEN
            NULL;
    END;

    PROCEDURE create_stat(v_period_name VARCHAR2) IS
        v_partition VARCHAR2(10) := 'p_' || REPLACE(v_period_name, '-', '');
    BEGIN
        dbms_stats.gather_table_stats(ownname => 'BIHR', tabname => 'REP_HEADCOUNT_MOVING_T', partname => v_partition, granularity => 'PARTITION');
    END;

    PROCEDURE REFRESH IS
        CURSOR c_period IS
            WITH periods AS
             (SELECT period_name
                    ,start_date
                    ,last_update_date
                    ,closing_status
              FROM (SELECT period_name
                          ,start_date
                          ,last_update_date
                          ,closing_status
                          ,row_number() OVER(PARTITION BY period_name ORDER BY last_update_date DESC) rn
                    FROM bimatview.gl_period_statuses t
                    WHERE application_id IN (801, 101) AND
                          adjustment_period_flag = 'N' AND
                          start_date >= to_date(' 01.01.2012', 'dd.mm.yyyy') AND
                          end_date < SYSDATE AND closing_status IN ('O', 'C'))
              WHERE rn = 1),
            periods_change AS
             (SELECT period_name
              FROM periods t
              WHERE closing_status = 'O' OR
                    (closing_status = 'C' AND NOT EXISTS
                     (SELECT 1
                      FROM bihr.rep_headcount_moving_insert_t
                      WHERE period_name = t.period_name AND
                            insert_date > last_update_date))),
            lead_periods AS
             (SELECT p.period_name
                    ,lead(p.period_name) OVER(ORDER BY p.start_date) lead_period_name
              FROM periods p)
            SELECT period_name
            FROM periods
            WHERE period_name IN (SELECT lead_period_name
                                  FROM lead_periods p
                                      ,periods_change ch
                                  WHERE ch.period_name = p.period_name AND
                                        lead_period_name IS NOT NULL
                                  UNION ALL
                                  SELECT period_name
                                  FROM periods_change)
            ORDER BY start_date;
    BEGIN
        FOR i IN c_period
        loop
            REFRESH(i.period_name);
        END loop;
    END;

    PROCEDURE REFRESH(v_period_name VARCHAR2) IS
    BEGIN
        dbms_application_info.set_action(action_name => v_period_name);
        DELETE FROM rep_headcount_moving_insert_t
        WHERE period_name = v_period_name;
        INSERT INTO rep_headcount_moving_insert_t
        VALUES
            (v_period_name
            ,SYSDATE);
        COMMIT;
        insert_tbl(v_period_name);
        update_tbl(v_period_name);
    END;

    PROCEDURE insert_tbl(v_period_name VARCHAR2) IS
    BEGIN
        set_partition(v_period_name);
        truncate_partition(v_period_name);
        bicommon_api_pkg.init_parameters('REP_HEADCOUNT_MOVING', v_period_name);
        INSERT INTO rep_headcount_moving_t
            SELECT * FROM rep_headcount_info_v;
        COMMIT;
        create_stat(v_period_name);
    END;

    PROCEDURE update_tbl(v_period_name VARCHAR2) AS
        CURSOR c_period_lag(v_period_name VARCHAR2) IS
            SELECT prev_period_name
                  ,mth_id
                  ,mth_desc_us
                  ,mth_start_dt
                  ,mth_end_dt
                  ,year_id
            FROM (SELECT period_name
                        ,LAG(period_name) OVER(ORDER BY period_year, period_num) prev_period_name
                        ,m.mth_id
                        ,m.mth_desc_us
                        ,m.mth_end_dt
                        ,m.mth_start_dt
                        ,year_id
                  FROM oebs_gl_periods p
                      ,lu_dt_month m
                  WHERE p.period_set_name = 'Бухгалтерский' AND
                        p.adjustment_period_flag = 'N' AND
                        p.start_date = m.mth_start_dt)
            WHERE period_name = v_period_name;
        v_period_lag c_period_lag%rowtype;
    BEGIN
        OPEN c_period_lag(v_period_name);
        fetch c_period_lag
            INTO v_period_lag;
        CLOSE c_period_lag;
        IF v_period_lag.prev_period_name IS NOT NULL AND
           exists_partition(v_period_lag.prev_period_name) > 0 THEN
            --чистим
            DELETE FROM rep_headcount_moving_t t
            WHERE period_name = v_period_name AND data_type = 'сторно';
            UPDATE bihr.rep_headcount_moving_t t
            SET t.cause = NULL
               ,t.gl_actual_headcount_delta = NULL
            WHERE period_name = v_period_name;
            --сторнируем
            MERGE INTO rep_headcount_moving_t t
            USING (select *
                   from rep_headcount_moving_t
                   where period_name = v_period_lag.prev_period_name) t_lag
            ON (t.period_name = v_period_name AND nvl(t.assignment_id, 0) = nvl(t_lag.assignment_id, 0) AND nvl(t_lag.concat_key, ' ') = nvl(t.concat_key, ' '))
            WHEN MATCHED THEN
                UPDATE
                SET t.gl_actual_headcount_delta = t.actual_headcount -
                                                  t_lag.actual_headcount
            WHEN NOT MATCHED THEN
                INSERT
                VALUES
                    (v_period_lag.mth_id
                    ,v_period_lag.mth_desc_us
                    ,v_period_lag.mth_end_dt
                    ,t_lag.party_id
                    ,t_lag.login
                    ,t_lag.full_name
                    ,v_period_lag.mth_start_dt
                    ,t_lag.assignment_id
                    ,t_lag.position_name
                    ,t_lag.position_num
                    ,t_lag.bp_status_code
                    ,t_lag.payment_type_name
                    ,v_period_lag.year_id
                    ,t_lag.location_name_ru
                    ,t_lag.position_grade_level
                    ,t_lag.profession
                    ,t_lag.assign_job_name
                    ,t_lag.assign_pay_basis_name
                    ,t_lag.assign_staff_rate
                    ,t_lag.product_value
                    ,t_lag.product_name
                    ,t_lag.leaf_dep_name
                    ,t_lag.dep_name_l2
                    ,t_lag.department_concat_hier
                    ,t_lag.segment1
                    ,t_lag.segment1_name
                    ,t_lag.segment2
                    ,t_lag.segment2_name
                    ,t_lag.segment3
                    ,t_lag.segment3_name
                    ,t_lag.segment4
                    ,t_lag.segment4_name
                    ,t_lag.segment5
                    ,t_lag.segment5_name
                    ,t_lag.segment6
                    ,t_lag.segment6_name
                    ,t_lag.segment7
                    ,t_lag.segment7_name
                    ,t_lag.segment8
                    ,t_lag.segment8_name
                    ,t_lag.segment9
                    ,t_lag.segment9_name
                    ,t_lag.segment10
                    ,t_lag.segment10_name
                    ,t_lag.segment11
                    ,t_lag.segment11_name
                    ,t_lag.segment12
                    ,t_lag.segment12_name
                    ,0
                    ,t_lag.bo_concat_hier
                    ,t_lag.bp_list
                    ,t_lag.bo_id
                    ,t_lag.level10_name
                    ,t_lag.level9_name
                    ,t_lag.level8_name
                    ,t_lag.level7_name
                    ,t_lag.level6_name
                    ,t_lag.level5_name
                    ,t_lag.level4_name
                    ,t_lag.level3_name
                    ,t_lag.level2_name
                    ,t_lag.level1_name
                    ,t_lag.bo_concat_hier_altern
                    ,t_lag.bo_id_altern
                    ,t_lag.level10_name_altern
                    ,t_lag.level9_name_altern
                    ,t_lag.level8_name_altern
                    ,t_lag.level7_name_altern
                    ,t_lag.level6_name_altern
                    ,t_lag.level5_name_altern
                    ,t_lag.level4_name_altern
                    ,t_lag.level3_name_altern
                    ,t_lag.level2_name_altern
                    ,t_lag.level1_name_altern
                    ,'сторно'
                    ,t_lag.is_maternity
                    ,t_lag.is_intern
                    ,t_lag.is_outstaff
                    ,t_lag.status
                    ,t_lag.position_id
                    ,t_lag.payment_type
                    ,t_lag.ch_assignment_status
                    ,t_lag.num_info1
                    ,t_lag.num_info9
                    ,t_lag.num_info10
                    ,t_lag.num_info2
                    ,t_lag.insert_date
                    ,v_period_name
                    ,''
                    ,-t_lag.actual_headcount
                    ,t_lag.replacement
                    ,t_lag.concat_key) WHERE
                    (t_lag.actual_headcount <> 0);
            ---gl_actual_headcount_delta
            UPDATE rep_headcount_moving_t t
            SET t.gl_actual_headcount_delta = t.actual_headcount
            WHERE period_name = v_period_name AND
                  t.gl_actual_headcount_delta IS NULL;
            --узнаем причину
            --найм
            UPDATE rep_headcount_moving_t t
            SET t.cause = 'найм'
            WHERE NOT EXISTS
             (SELECT 1
                   FROM rep_headcount_moving_t t_lag
                   WHERE t_lag.party_id = t.party_id AND
                         data_type <> 'сторно' AND
                         period_name = v_period_lag.prev_period_name) AND
                  t.party_id IS NOT NULL AND period_name = v_period_name AND
                  EXISTS (SELECT 1
                   FROM rep_headcount_moving_t
                   WHERE party_id = t.party_id AND
                         period_name = v_period_name AND
                         gl_actual_headcount_delta <> 0);
            --увольнение
            UPDATE rep_headcount_moving_t t
            SET t.cause = 'увольнение'
            WHERE NOT EXISTS
             (SELECT 1
                   FROM rep_headcount_moving_t
                   WHERE party_id = t.party_id AND
                         period_name = v_period_name AND
                         data_type <> 'сторно') AND t.party_id IS NOT NULL AND
                  period_name = v_period_name AND EXISTS
             (SELECT 1
                   FROM rep_headcount_moving_t
                   WHERE party_id = t.party_id AND
                         period_name = v_period_name AND
                         gl_actual_headcount_delta <> 0);
            --изменения по ручным загрузкам
            UPDATE rep_headcount_moving_t t
            SET t.cause = 'изменения по ручным загрузкам'
            WHERE t.party_id IS NULL AND period_name = v_period_name AND
                  EXISTS (SELECT 1
                   FROM rep_headcount_moving_t
                   WHERE party_id = t.party_id AND
                         period_name = v_period_name AND
                         gl_actual_headcount_delta <> 0);
            --стажеры/декрет/Outstaff/изменение численности
            UPDATE rep_headcount_moving_t tt
            SET tt.cause =
                (SELECT CASE
                            WHEN t.is_intern = 0 AND t_lag.is_intern > 0 THEN
                             'найм стажера'
                            WHEN t.is_intern > 0 AND t_lag.is_intern = 0 THEN
                             'увольнение в стажеры'
                            WHEN t.is_maternity > 0 AND
                                 t_lag.is_maternity = 0 THEN
                             'уход в декрет'
                            WHEN t.is_maternity = 0 AND
                                 t_lag.is_maternity > 0 THEN
                             'выход из декрета'
                            WHEN t.is_outstaff = 0 AND t_lag.is_outstaff > 0 THEN
                             'найм из Outstaff'
                            WHEN t.is_outstaff > 0 AND t_lag.is_outstaff = 0 THEN
                             'увольнение в Outstaff'
                            WHEN t.actual_headcount <> t_lag.actual_headcount THEN
                             'изменение численности'
                            WHEN regexp_replace(t.bp_list, '([^;]+)(;\1)?+', '\1') =
                                 regexp_replace(t_lag.bp_list, '([^;]+)(;\1)?+', '\1') THEN
                             'перевод c бюджетом'
                            ELSE
                             'перевод без бюджетa'
                        END cause
                 FROM (SELECT sum(is_intern) is_intern
                             ,sum(is_maternity) is_maternity
                             ,sum(is_outstaff) is_outstaff
                             ,sum(actual_headcount) actual_headcount
                             ,listagg(bp_list, ';') WITHIN GROUP(ORDER BY bp_list) AS bp_list
                       FROM rep_headcount_moving_t
                       WHERE party_id = tt.party_id AND
                             period_name = v_period_name AND
                             data_type <> 'сторно') t
                     ,(SELECT sum(is_intern) is_intern
                             ,sum(is_maternity) is_maternity
                             ,sum(is_outstaff) is_outstaff
                             ,sum(actual_headcount) actual_headcount
                             ,listagg(bp_list, ';') WITHIN GROUP(ORDER BY bp_list) AS bp_list
                       FROM rep_headcount_moving_t
                       WHERE party_id = tt.party_id AND
                             data_type <> 'сторно' AND
                             period_name = v_period_lag.prev_period_name) t_lag)
            WHERE tt.cause IS NULL AND period_name = v_period_name AND
                  EXISTS (SELECT 1
                   FROM rep_headcount_moving_t
                   WHERE party_id = tt.party_id AND
                         period_name = v_period_name AND
                         gl_actual_headcount_delta <> 0);
            /*    --перевод
            UPDATE rep_headcount_moving_t tt
            SET tt.cause = decode(data_type, 'сторно', 'перевод из ', 'перевод в ') ||
                           trim(tt.cause)
            WHERE period_name = v_period_name AND
                  tt.cause IN ('c бюджетом', 'без бюджетa');*/
        END IF;
        --новый/замена
        UPDATE (SELECT t.cause
                      ,repl
                FROM rep_headcount_moving_t t
                    ,(SELECT h.party_id
                            ,decode(SUM(instr(upper(s.bp_status_code), 'REPLACEMENT')), 0, 'новый', 'замена') repl
                      FROM oebs_xxhr_bi_bp_status_tbl s
                          ,rep_headcount_moving_t h
                      WHERE v_period_lag.mth_end_dt BETWEEN
                            s.date_from AND s.date_to and
                            s.assignment_id = h.assignment_id and
                            s.position_id = h.position_id and
                            h.period_name =v_period_name
                      GROUP BY h.party_id) h
                WHERE t.period_name = v_period_name AND t.cause IS NOT NULL AND
                      t.party_id = h.party_id)
        SET cause = TRIM(cause) || ' ' || repl;
        COMMIT;
    END;

END;
/
