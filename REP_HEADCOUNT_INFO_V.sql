CREATE OR REPLACE VIEW BIHR.REP_STAFFCABINET_V AS
WITH eff_date AS (
  SELECT
    trunc(SYSDATE, 'MONTH') -1 effective_date
  FROM
    dual
),
logins AS ( --список login для выборки
  SELECT  /*+ MATERIALIZE */
    DISTINCT x.login
  FROM
    oebs_xxhr_assignments x,
    eff_date e
  WHERE
    x.login IS NOT NULL
    AND x.login NOT IN (
      SELECT
        login
      FROM
        rep_staffcabinet_login_t
      WHERE
        TYPE = 'all'
    )
    AND x.assignment_status_type_id !=3
    AND e.effective_date BETWEEN x.effective_start_date
    AND x.effective_end_date
),
login_assignment_id AS ( --списк assignment_id для выборки payment
  SELECT
    DISTINCT login,
    assignment_id
  FROM
    oebs_xxhr_assignments x
  WHERE
    EXISTS (
      SELECT
        1
      FROM
        logins
      WHERE
        login = x.login
    )
    AND x.assignment_status_type_id != 3
),
xxbud_bg_priority AS ( --определение priority для пределения main assignment
  SELECT
    to_number(fv.hierarchy_level) hierarchy_level,
    to_number(fv.flex_value) flex_value
  FROM
    oebs_fnd_flex_value_sets fvs
    INNER JOIN oebs_fnd_flex_values fv ON fvs.flex_value_set_id = fv.flex_value_set_id
  WHERE
    fvs.flex_value_set_name = 'XXBUD_BG_PRIORITY'
),
contr1 AS ( --contrac info
  SELECT
    pc2.ctr_contract_id,
    '№ ' || pc2.ctr_contract_number || ' от ' || nvl(
      to_char(
        pc2.ctr_sign_day_dt, 'DD.MM.YYYY'
      ),
      to_char(
        pc2.ctr_effective_start_date, 'DD.MM.YYYY'
      )
    ) ctr_contract_number,
    pc2.ctr_planned_ending_dt
  FROM
    (
      SELECT
        pc1.contract_id ctr_contract_id,
        pc1.REFERENCE ctr_contract_number,
        decode(
          pc1.attribute_category,
          'TD',
          bicommon_api_pkg.oebs_date(pc1.attribute2)
        ) ctr_planned_ending_dt,
        decode(
          pc1.attribute_category,
          'TD',
          bicommon_api_pkg.oebs_date(pc1.attribute1)
        ) ctr_sign_day_dt,
        pc1.effective_start_date ctr_effective_start_date,
        row_number() OVER(
          PARTITION BY pc1.contract_id
          ORDER BY
            pc1.effective_start_date
        ) rn
      FROM
        oebs_per_contracts_f pc1
    ) pc2
  WHERE
    pc2.rn = 1
),
assignment_info2 AS ( --данные из assignment_info2
  SELECT
    A.assignment_id,
    A.login,
    A.assignment_number,
    A.person_id,
    A.party_id,
    nvl(
      contr1.ctr_contract_number, bicommon_api_pkg.empty_data_ru
    ) contract_number,
    nvl(
      le.legal_entity_name_ru, bicommon_api_pkg.empty_data_ru
    ) legal_entity_name,
    nvl(
      j.attribute1, bicommon_api_pkg.empty_data_ru
    ) job_name,
    le.legal_entity_id,
    A.business_group_id
  FROM
    f_assignment_info2_v A,
    eff_date e,
    contr1,
    lu_legal_entity le,
    oebs_per_jobs j
  WHERE
    EXISTS (
      SELECT
        1
      FROM
        logins
      WHERE
        login = A.login
    )
    AND e.effective_date BETWEEN A.effective_start_date
    AND A.effective_end_date
    AND A.contract_id = contr1.ctr_contract_id(+)
    AND A.legal_entity_id = le.legal_entity_id(+)
    AND A.job_id = j.job_id(+)
),
assignments AS ( --основной запрос по assignment
  SELECT
    A.login,
    A.assignment_id,
    ai2.assignment_number,
    ai2.contract_number,
    ai2.legal_entity_name,
    ai2.job_name,
    ai2.legal_entity_id,
    ai2.business_group_id,
    A.effective_start_date,
    A.effective_end_date,
    A.assignment_status_type_id,
    A.party_id,
    A.position_id,
    A.conc_request_id,
    A.conc_request_start_date,
    A.per_system_status,
    A.person_id,
    A.transfer_status,
    A.asg_group,
    A.main_asg_group_flag,
    CASE WHEN A.main_asg_flag = 'Y' THEN decode(
      MIN(
        nvl(fv.hierarchy_level, 100)
      ) OVER(
        PARTITION BY A.login, A.main_asg_flag
      ),
      nvl(fv.hierarchy_level, 100),
      'Y',
      'N'
    ) ELSE 'N' END main_asg_flag
  FROM
    oebs_xxhr_assignments A,
    eff_date e,
    assignment_info2 ai2,
    xxbud_bg_priority fv
  WHERE
    EXISTS (
      SELECT
        1
      FROM
        logins
      WHERE
        login = A.login
    )
    AND A.assignment_status_type_id != 3
    AND e.effective_date BETWEEN A.effective_start_date
    AND A.effective_end_date
    AND A.assignment_id = ai2.assignment_id(+)
    AND ai2.business_group_id = fv.flex_value(+)
),
salary AS ( --зарплата  по assignment_id
  SELECT
    y.assignment_id,
    y.rep_currency_id,
    d.universal_salary_amount,
    y.rep_salary_rate
  FROM
    rep_staffcabinet_det_t d,
    f_salary y,
    eff_date e
  WHERE
  d.assignment_id=y.assignment_id
  AND e.effective_date BETWEEN d.from_day_dt  AND d.to_day_dt
  AND e.effective_date BETWEEN y.from_day_dt  AND y.to_day_dt
),
period AS ( --периодны
  SELECT
    to_number(to_char(SYSDATE, 'YYYY')) - to_number(year_id) + 1 year_nr,
    year_start_dt dfrom_cal,
    least( year_end_dt, trunc(SYSDATE, 'MONTH') -1 ) dto_cal,
    add_months(last_day(to_date('01' || to_char(trunc(SYSDATE, 'MONTH') -1,  '.mm.' ) || year_desc,   'dd.mm.yyyy' ) ), -12) + 1 dfrom_eff,
    last_day( to_date('01' || to_char(  trunc(SYSDATE, 'MONTH') -1,   '.mm.') || year_desc,  'dd.mm.yyyy' ) ) dto_eff
  FROM
    lu_dt_year
  WHERE
    add_months( trunc(SYSDATE, 'MONTH') -1,  -48 ) < year_start_dt
    AND year_start_dt <= trunc(SYSDATE, 'MONTH') -1
  UNION ALL
  SELECT
    0,
    trunc(SYSDATE, 'mm'),
    year_end_dt,
    trunc(SYSDATE, 'mm'),
    add_months(trunc(SYSDATE, 'mm'),  12 ) -1
  FROM
    lu_dt_year
  WHERE
    SYSDATE BETWEEN year_start_dt
    AND year_end_dt
),
/*default_exchange_rate_type AS (--тип конвертации по assignment_id
  SELECT
    A.assignment_id,
    ct.user_conversion_type
  FROM
    oebs_per_assignments A,
    oebs_hr_soft_coding_kf sck,
    (
      SELECT
        aspa.org_id,
        aspa.default_exchange_rate_type
      FROM
        oebs_ap_system_parameters_all aspa
    ) p,
    oebs_gl_daily_conversion_types ct
  WHERE
    A.soft_coding_keyflex_id = sck.soft_coding_keyflex_id(+)
    AND sck.segment1 = p.org_id(+)
    AND p.default_exchange_rate_type = ct.conversion_type
  GROUP BY
    A.assignment_id,
    ct.user_conversion_type
),*/
rate AS ( --курсы
   SELECT
      distinct  legal_entity_convert_type conversion_type
      ,conversion_day conversion_day_dt
      ,conversion_day_to conversion_day_dt_to
      ,from_currency_id
      ,to_currency_id
      ,rate conversion_rate
   FROM
      LEGAL_ENTITY_CURRENCY_RATE
),
review AS ( --информация о ревью для прогноза
  SELECT
    trunc(
      add_months(xir.review_start_date, 2),
      'mm'
    ) look_date,
    mth_id look_mth_id,
    xir.subject_login,
    xir.salary_after_review_val,
    xir.bonus_absolute,
    xir.bonus_rsu
  FROM
    oebs_xxhr_intf_review xir,
    lu_dt_month m
  WHERE
    trunc(SYSDATE) BETWEEN xir.review_start_date  AND trunc(  add_months(xir.review_start_date, 3),  'month'  )
    AND trunc( add_months(xir.review_start_date, 2),  'mm' )= mth_start_dt
    AND decode (xir.status, 'announced', 1, 0)= 1
),
parent_assignment AS ( -- данные по main assignment
  SELECT
    A.login,
    sd.assignment_id,
    sd.rep_currency_id,
    sd.universal_salary_amount,
    nvl(sd.user_conversion_type, 'Курс ЦБ РФ') conversion_type
  FROM
    rep_staffcabinet_det_t sd,
    assignments A,
    eff_date e--,
--    default_exchange_rate_type d
  WHERE
    e.effective_date BETWEEN sd.from_day_dt AND sd.to_day_dt
    AND A.main_asg_flag = 'Y'
    AND sd.assignment_id = A.assignment_id
  --  AND A.assignment_id = d.assignment_id(+)
),
period_min_max AS ( --определение всего периода сбора данных
  SELECT
    MIN(dfrom_eff) dfrom_eff,
    MAX(dto_eff) dto_eff
  FROM
    period
),
parent_for_assigment_id AS ( --привялзка всех отбираемых assignment к  main assignment
  SELECT
    A.assignment_id,
    pa.assignment_id pa_assignment_id,
    pa.rep_currency_id pa_rep_currency_id,
    A.login,
    pa.conversion_type
  FROM
    login_assignment_id A,
    parent_assignment pa
  WHERE
    A.login = pa.login
  GROUP BY
    A.assignment_id,
    pa.assignment_id,
    pa.rep_currency_id,
    A.login,
    pa.conversion_type
),
payment_crnc AS ( --выборка payment сгруппировано по дате в валюте назначения
  SELECT
    assignment_id,
    payment_day_dt,
    sum(amount) amount,
    payment_tp,
    currency_id,
    main_currency_id,
    login,
    conversion_type
  FROM
      ( SELECT
      p.assignment_id,
      p.payment_day_dt,
      p.amount,
      decode(  l.lookup_type, 'REP_STAFFCABINET_V_OTHER',  'other', 'REP_STAFFCABINET_V_BONUS','bonus' ,'piecerate') payment_tp,
      p.currency_id,
      pa.pa_rep_currency_id main_currency_id,
      pa.login,
      pa.conversion_type
      FROM
      f_payment p,
      period_min_max pmm,
      parent_for_assigment_id pa,
      (
        SELECT
        lookup_type,
        description
        FROM
        report_lookup_values_tbl
        GROUP BY
        lookup_type,
        description
      ) l
      WHERE
      p.element_group_name_ru = 'Budgetary Control Bonus'
      AND p.tax <> 'A'
      AND p.payment_day_dt BETWEEN pmm.dfrom_eff
      AND pmm.dto_eff
      AND p.payment_day_dt <= trunc(SYSDATE, 'mm')-1 --в прогнозе не показываем
      AND p.assignment_id = pa.assignment_id
      AND regexp_replace(  trim(p.element_name_ru),  '  *',   ' ') = l.description
      AND l.lookup_type IN ( 'REP_STAFFCABINET_V_BONUS', 'REP_STAFFCABINET_V_OTHER','REP_STAFFCABINET_V_PIECERATE')
      )
  GROUP BY
    assignment_id,
    payment_day_dt,
    currency_id,
    payment_tp,
    main_currency_id,
    login,
    conversion_type
),
payment AS (-- payment переводим в валюту основного назначения
  SELECT
    p.payment_day_dt,
    p.amount * r.conversion_rate amount,
    p.payment_tp,
    p.login
  FROM
    payment_crnc p,
    rate r
  WHERE
    p.currency_id <> p.main_currency_id
    AND p.currency_id = r.from_currency_id(+)
    AND p.main_currency_id = r.to_currency_id (+)
    AND last_day(  add_months( trunc(SYSDATE),   -1) ) BETWEEN r.conversion_day_dt(+)  AND r.conversion_day_dt_to (+)
    AND r.conversion_type = p.conversion_type
  UNION ALL
  SELECT
    payment_day_dt,
    amount,
    payment_tp,
    login
  FROM
    payment_crnc
  WHERE
    currency_id = main_currency_id
  UNION ALL
  SELECT
    r.look_date,
    bonus_absolute,
    'bonus',
    subject_login
  FROM
    review r
  WHERE
    nvl(bonus_rsu, 0) = 0
)
, payment_year_nr AS (-- разбиваем премию по годам
  SELECT
    login,
    round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_eff_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_eff_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_eff_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_eff_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF' AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_eff_sum_4,
    round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL' AND payment_tp='bonus'  THEN amount ELSE 0 END ),0),2) bonus_cal_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL' AND payment_tp='bonus'  THEN amount ELSE 0 END ),0),2) bonus_cal_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL'  AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_cal_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL' AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_cal_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' AND payment_tp='bonus' THEN amount ELSE 0 END ),0),2) bonus_cal_sum_4,
	round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_eff_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_eff_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_eff_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_eff_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF' AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_eff_sum_4,
    round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL' AND payment_tp='piecerate'  THEN amount ELSE 0 END ),0),2) piecerate_cal_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL' AND payment_tp='piecerate'  THEN amount ELSE 0 END ),0),2) piecerate_cal_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL'  AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_cal_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL' AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_cal_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' AND payment_tp='piecerate' THEN amount ELSE 0 END ),0),2) piecerate_cal_sum_4,
    round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_eff_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_eff_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_eff_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_eff_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF' AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_eff_sum_4,
    round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL' AND payment_tp='other'  THEN amount ELSE 0 END ),0),2) other_cal_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL' AND payment_tp='other'  THEN amount ELSE 0 END ),0),2) other_cal_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL'  AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_cal_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL' AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_cal_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' AND payment_tp='other' THEN amount ELSE 0 END ),0),2) other_cal_sum_4
  FROM
    (SELECT
            year_nr, login,amount,'CAL' tp,payment_tp  FROM period, payment  s WHERE payment_day_dt BETWEEN dfrom_cal AND dto_cal
    UNION ALL
    SELECT
          year_nr, login,amount,'EFF' tp,payment_tp  FROM  period, payment  s WHERE  payment_day_dt BETWEEN dfrom_eff AND dto_eff)
    GROUP BY
          login)
, salary_login AS ( --текущий оклад
  SELECT
    login,
    round( sum(universal_salary_amount * nvl(conversion_rate, 1)), 2) salary_amount,
    main_rep_currency_id
  FROM
    rep_staffcabinet_det_t
  WHERE
    trunc(SYSDATE, 'MONTH') -1 BETWEEN from_day_dt
    AND to_day_dt
  GROUP BY
    login,
    main_rep_currency_id
),
salary_mth_review AS ( --зарплата + повышение по ревью по месяца разбито
  SELECT
    mth_id,
    login,
    nvl(salary_after_review_val, salary_amount) salary_amount,
    salary_after_review_val
  FROM
    (
      SELECT
        login,
        mth_id,
        sum(salary_amount) salary_amount
      FROM
        rep_staffcabinet_det_t
      GROUP BY
        login,
        mth_id
    ) s,
    review
  WHERE
    login = subject_login(+)
    AND mth_id >= look_mth_id(+)
)
, salary_year_nr AS --разбиваем зарплату по годам
(
  SELECT
    login,
    round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' THEN salary_amount ELSE 0 END ),0) ,2)salary_eff_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' THEN salary_amount ELSE 0 END ),0) ,2)salary_eff_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' THEN salary_amount ELSE 0 END ),0) ,2)salary_eff_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' THEN salary_amount ELSE 0 END ),0) ,2)salary_eff_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF' THEN salary_amount ELSE 0 END ),0) ,2)salary_eff_sum_4,
    round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL' THEN salary_amount ELSE 0 END ),0) ,2)salary_cal_sum_01,
    round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL' THEN salary_amount ELSE 0 END ),0) ,2)salary_cal_sum_1,
    round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL' THEN salary_amount ELSE 0 END ),0) ,2)salary_cal_sum_2,
    round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL' THEN salary_amount ELSE 0 END ),0) ,2)salary_cal_sum_3,
    round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' THEN salary_amount ELSE 0 END ),0) ,2) salary_cal_sum_4
  FROM
  (SELECT
      year_nr, login,salary_amount,'CAL' tp  FROM lu_dt_month m, period, salary_mth_review s WHERE m.mth_start_dt>=dfrom_cal AND m.mth_end_dt<=dto_cal
   AND
      m.mth_id=s.mth_id
   UNION ALL
   SELECT
      year_nr, login,salary_amount,'EFF' tp  FROM lu_dt_month m, period, salary_mth_review s WHERE m.mth_start_dt>=dfrom_eff AND m.mth_end_dt<=dto_eff
   AND
     m.mth_id=s.mth_id)
  GROUP BY login
)
, price AS --цена вестинга
 (
 SELECT attribute1 class_code
        ,start_date_active
        ,nvl(end_date_active, to_date('01.01.4000', 'dd.mm.yyyy')) end_date_active
        ,bicommon_api_pkg.oebs_number(description) price
  FROM oebs_fnd_lookup_values f
  WHERE f.lookup_type = 'XXHR_O_GRANT_PRICE' AND language = 'RU'
  UNION ALL
  SELECT class_code
        ,start_date_active
        ,end_date_active
        ,price
  FROM (SELECT course_dt start_date_active
              ,lead(course_dt, 1, to_date('01.01.4000', 'dd.mm.yyyy')) OVER(ORDER BY course_dt) - 1 end_date_active
              ,course_close price
        FROM bicommon.f_share_price)
      ,(SELECT lookup_code class_code
       FROM bihr.oebs_fnd_lookup_values f
       WHERE lookup_type = 'XXHR_O_CLASS_CODES' AND language = 'US' AND
             attribute1 = '202'))
,vesting_crnc AS ( --отбор вестингов сгруппировано по дате
  SELECT
    login,
    vest_legal_entity_name,
    schedule_date,
    sum(amount) amount,
    grant_currency
  FROM
  (
      SELECT
      lower(t.login) login,
      le.meaning vest_legal_entity_name,
      s.schedule_date,
      greatest( ( nvl(p.price,0) - d.grant_price ), 0 ) * s.schedule_amount amount,
      grant_currency,
      t.entity_id
      FROM
      oebs_xxhr_o_holders t,
      oebs_xxhr_o_grants d,
      oebs_xxhr_o_vesting_schedule v,
      oebs_xxhr_o_schedules s,
      period_min_max period,
      (
        SELECT
        lookup_code,
        meaning
        FROM
        oebs_fnd_lookup_values
        WHERE
        lookup_type = 'XXHR_O_ENTITYES'
        AND language = 'US'
      ) le,
      price p,
      logins l
      WHERE
       t.holder_id = d.holder_id
      AND d.grant_status NOT LIKE '%DELETED%'
      AND d.grant_status NOT LIKE '%NOT_AGREED%'
      AND d.declared = 'Y'
      AND trunc(v.date_to) = bicommon_api_pkg.oebs_hr_max_date
      AND (
        EXISTS ( SELECT  1  FROM  oebs_xxhr_o_grant_deals g WHERE   d.holder_id = g.holder_id  AND g.grant_id = d.grant_id AND grant_id_to IS NULL )
        OR NOT EXISTS ( SELECT  1  FROM  oebs_xxhr_o_grant_deals g  WHERE  d.holder_id = g.holder_id  AND g.grant_id = d.grant_id )
                )
      AND d.grant_id = v.grant_id
      AND v.schedule_status NOT LIKE '%DELETED%'
      AND v.schedule_status NOT LIKE '%NOT_AGREED%'
      AND v.xxhr_o_vesting_schedule_id = s.xxhr_o_vest_realiz_sch_id
      AND s.schedule_date BETWEEN period.dfrom_eff
      AND period.dto_eff
      AND t.entity_id = le.lookup_code(+)
      AND d.class_code=p.class_code(+)
      AND s.schedule_date between p.start_date_active(+) and p.end_date_active(+)
      AND l.login = lower(t.login)
  )
  GROUP BY
    login,
    vest_legal_entity_name,
    schedule_date,
    grant_currency
)
, vesting AS ( --перевод вестингов в валюту основного контракта
  SELECT
    v.login,
    v.vest_legal_entity_name,
    v.schedule_date,
    v.amount
  FROM
    vesting_crnc v,
    parent_assignment p
  WHERE
    v.login = p.login
    AND v.grant_currency = p.rep_currency_id
  UNION ALL
  SELECT
    v.login,
    v.vest_legal_entity_name,
    v.schedule_date,
    v.amount * r.conversion_rate
  FROM
    vesting_crnc v,
    parent_assignment p,
    rate r
  WHERE
    v.login = p.login
    AND v.grant_currency <> p.rep_currency_id
    AND v.grant_currency = r.from_currency_id
    AND p.rep_currency_id = r.to_currency_id
    AND v.schedule_date BETWEEN r.conversion_day_dt
    AND r.conversion_day_dt_to
    AND r.conversion_type = p.conversion_type
)
, vesting_year_nr AS ( --вестинги группируем по годам и login + vest_legal_entity_name
  SELECT
     login,vest_legal_entity_name,
     round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' THEN amount ELSE 0 END ),0),2) vest_le_eff_sum_01,
     round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' THEN amount ELSE 0 END ),0),2) vest_le_eff_sum_1,
     round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' THEN amount ELSE 0 END ),0),2) vest_le_eff_sum_2,
     round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' THEN amount ELSE 0 END ),0),2) vest_le_eff_sum_3,
     round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF'  THEN amount ELSE 0 END ),0),2) vest_le_eff_sum_4,
     round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL'  THEN amount ELSE 0 END ),0),2) vest_le_cal_sum_01,
     round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL'  THEN amount ELSE 0 END ),0),2) vest_le_cal_sum_1,
     round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL'  THEN amount ELSE 0 END ),0),2) vest_le_cal_sum_2,
     round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL'  THEN amount ELSE 0 END ),0),2) vest_le_cal_sum_3,
     round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' THEN amount ELSE 0 END ),0),2) vest_le_cal_sum_4
  FROM
  (SELECT
     year_nr, login,amount,'CAL' tp ,vest_legal_entity_name FROM period, vesting  s WHERE schedule_date BETWEEN dfrom_cal AND dto_cal
   UNION ALL
   SELECT
     year_nr, login,amount,'EFF' tp ,vest_legal_entity_name FROM  period, vesting  s WHERE  schedule_date BETWEEN dfrom_eff AND dto_eff)
   GROUP BY
     login,vest_legal_entity_name
)
, vesting_login_year_nr AS ( --вестинги группируем по годам и login
  SELECT login,
    sum(vest_le_eff_sum_01) vest_eff_sum_01,
    sum(vest_le_eff_sum_1) vest_eff_sum_1,
    sum(vest_le_eff_sum_2) vest_eff_sum_2,
    sum(vest_le_eff_sum_3) vest_eff_sum_3,
    sum(vest_le_eff_sum_4) vest_eff_sum_4,
    sum(vest_le_cal_sum_01) vest_cal_sum_01,
    sum(vest_le_cal_sum_1) vest_cal_sum_1,
    sum(vest_le_cal_sum_2) vest_cal_sum_2,
    sum(vest_le_cal_sum_3) vest_cal_sum_3,
    sum(vest_le_cal_sum_4) vest_cal_sum_4
  FROM vesting_year_nr
  GROUP BY login),
period_year_nr AS ( --информация о периодах
  SELECT
     MAX(scal_01) scal_01,
     MAX(scal_1) scal_1,
     MAX(scal_2) scal_2,
     MAX(scal_3) scal_3,
     MAX(scal_4) scal_4,
     MAX(seff_beg_01) seff_beg_01,
     MAX(seff_beg_1) seff_beg_1,
     MAX(seff_beg_2) seff_beg_2,
     MAX(seff_beg_3) seff_beg_3,
     MAX(seff_beg_4) seff_beg_4,
     MAX(seff_end_01) seff_end_01,
     MAX(seff_end_1) seff_end_1,
     MAX(seff_end_2) seff_end_2,
     MAX(seff_end_3) seff_end_3,
     MAX(seff_end_4) seff_end_4
  FROM
  (
     SELECT
        (CASE WHEN year_nr=0 THEN dfrom_cal ELSE NULL END) scal_01
       ,(CASE WHEN year_nr=1 THEN dfrom_cal ELSE NULL END) scal_1
       ,(CASE WHEN year_nr=2 THEN dfrom_cal ELSE NULL END) scal_2
       ,(CASE WHEN year_nr=3 THEN dfrom_cal ELSE NULL END) scal_3
       ,(CASE WHEN year_nr=4 THEN dfrom_cal ELSE NULL END) scal_4
       ,(CASE WHEN year_nr=0 THEN dfrom_eff ELSE NULL END) seff_beg_01
       ,(CASE WHEN year_nr=1 THEN dfrom_eff ELSE NULL END) seff_beg_1
       ,(CASE WHEN year_nr=2 THEN dfrom_eff ELSE NULL END) seff_beg_2
       ,(CASE WHEN year_nr=3 THEN dfrom_eff ELSE NULL END) seff_beg_3
       ,(CASE WHEN year_nr=4 THEN dfrom_eff ELSE NULL END) seff_beg_4
       ,(CASE WHEN year_nr=0 THEN dto_eff ELSE NULL END) seff_end_01
       ,(CASE WHEN year_nr=1 THEN dto_eff ELSE NULL END) seff_end_1
       ,(CASE WHEN year_nr=2 THEN dto_eff ELSE NULL END) seff_end_2
       ,(CASE WHEN year_nr=3 THEN dto_eff ELSE NULL END) seff_end_3
       ,(CASE WHEN year_nr=4 THEN dto_eff ELSE NULL END) seff_end_4
    FROM period)
),
loan_crnc AS ( --отбор 'Sign up', 'Sign up (2 года)',  'Retention'
  SELECT
    loan_contr_day_dt,
    loan_type_name_ru,
    sum(loan_amount)/ 0.87 loan_amount,
    currency_id,
    assignment_id,
    login
  FROM
     (
  SELECT
    t1.loan_contr_day_dt,
    t1.loan_type_name_ru,
    t1.loan_amount,
    t1.currency_id,
    t1.assignment_id,
    t1.login
  FROM
      (
        SELECT
        fa.assignment_id,
        fa.person_info_id,
        l.loan_id,
        fa.full_name_ru,
        fa.login,
        fa.assignment_hire_day2_dt,
        bg.business_group_name_ru,
        le.legal_entity_name_ru,
        ast.assignment_status_type_name_ru,
        round(
          (
          (l.loan_contr_day_dt) - trunc(fa.assignment_hire_day2_dt)
          ) / 365,
          1
        ) work_period,
        CASE WHEN lt.loan_type_name_ru = 'Sign up (2 года)'
        AND l.additional_agreement_day_dt IS NOT NULL THEN add_months(l.loan_contr_day_dt, 12) ELSE l.loan_contr_day_dt END loan_contr_day_dt,
        l.loan_contract_number,
        CASE WHEN lt.loan_type_name_ru = 'Sign up (2 года)' THEN   DECODE (l.additional_agreement_day_dt, NULL, 'Sign up' , 'Sign up (2 года)')
               ELSE lt.loan_type_name_ru
             END  loan_type_name_ru,
        l.loan_amount,
        l.currency_id,
        bg.position_grade_name_ru,
        l.loan_month,
        ddd.dep_name
        FROM
        f_assignment_info fa
        JOIN logins l ON (fa.login = l.login)
        JOIN lu_assignment_status_type ast ON (
          fa.assignment_status_type_id = ast.assignment_status_type_id
        )
        JOIN lu_legal_entity le ON (
          fa.legal_entity_id = le.legal_entity_id
        )
        JOIN lu_business_group bg ON (
          fa.business_group_id = bg.business_group_id
        )
        JOIN f_loan_info_v l ON (fa.person_info_id = l.person_id)
        JOIN lu_loan_type lt ON (l.loan_type_id = lt.loan_type_id)
        JOIN lu_department_hier dh ON (
          fa.department_hier_id = dh.department_hier_id
        )
        JOIN lu_dep_department_v ddd ON (
          dh.a_dep_info_id = ddd.dep_info_id
        )
        LEFT JOIN f_salary s ON (
          fa.assignment_id = s.assignment_id
          AND trunc(SYSDATE) BETWEEN s.from_day_dt
          AND s.to_day_dt
        )
        LEFT JOIN lu_bp_grade bg ON (
          s.assignment_grade_id = bg.position_grade_id
        ),
        period_min_max
        WHERE
        dfrom_eff <= loan_contr_day_dt
        AND fa.irc_flag = 0
        AND fa.people_group_id IN (10, 11, 12)
        AND trunc(SYSDATE) BETWEEN fa.from_day_dt
        AND fa.to_day_dt
        AND lt.loan_type_name_ru != 'Нет данных'
      ) t1
      LEFT JOIN (
        SELECT
        MAX(pa.payment_num) max_payment_num,
        pa.person_id,
        pa.assignment_id,
        pa.loan_id
        FROM
        f_loan_payment pa
        GROUP BY
        pa.person_id,
        pa.assignment_id,
        pa.loan_id
      ) t2 ON (
        t1.assignment_id = t2.assignment_id
        AND t1.loan_id = t2.loan_id
      )
      LEFT JOIN f_loan_payment pa2 ON (
        t1.assignment_id = pa2.assignment_id
        AND t1.loan_id = pa2.loan_id
        AND t2.max_payment_num = pa2.payment_num
      )
      WHERE
      pa2.saldo_main_amount IS NOT NULL
      AND t1.assignment_status_type_name_ru != 'Назнач. прекращ.'
      AND t1.loan_type_name_ru IN (
        'Sign up', 'Sign up (2 года)',
        'Retention'
      )
    )
  GROUP BY
    loan_contr_day_dt,
    loan_type_name_ru,
    currency_id,
    assignment_id,
    login
),
loan AS ( --перевод в валюту основного назначения 'Sign up', 'Sign up (2 года)', 'Retention'
  SELECT
    v.login,
    v.loan_type_name_ru,
    v.loan_contr_day_dt,
    v.loan_amount loan_amount
  FROM
    loan_crnc v,
    parent_assignment p
  WHERE
    v.login = p.login
    AND v.currency_id = p.rep_currency_id
  UNION ALL
  SELECT
    v.login,
    v.loan_type_name_ru,
    v.loan_contr_day_dt,
    v.loan_amount * r.conversion_rate
  FROM
    loan_crnc v,
    parent_assignment p,
    rate r
  WHERE
    v.login = p.login
    AND v.currency_id <> p.rep_currency_id
    AND v.loan_contr_day_dt BETWEEN r.conversion_day_dt
    AND r.conversion_day_dt_to
    AND v.currency_id = r.from_currency_id
    AND p.rep_currency_id = r.to_currency_id
    AND r.conversion_type = p.conversion_type
)
 , loan_year_nr AS ( --разбивка по годам 'Sign up', 'Sign up (2 года)',  'Retention'
   SELECT
        login,
        round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' AND loan_type_name_ru='Sign up'  THEN loan_amount ELSE 0 END ),0),2) signup_eff_sum_01,
        round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' AND loan_type_name_ru='Sign up'  THEN loan_amount ELSE 0 END ),0),2) signup_eff_sum_1,
        round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' AND loan_type_name_ru='Sign up'  THEN loan_amount ELSE 0 END ),0),2) signup_eff_sum_2,
        round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' AND loan_type_name_ru='Sign up'  THEN loan_amount ELSE 0 END ),0),2) signup_eff_sum_3,
        round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF' AND loan_type_name_ru='Sign up'   THEN loan_amount ELSE 0 END ),0),2) signup_eff_sum_4,
        round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL' AND loan_type_name_ru='Sign up'   THEN loan_amount ELSE 0 END ),0),2) signup_cal_sum_01,
        round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL' AND loan_type_name_ru='Sign up'   THEN loan_amount ELSE 0 END ),0),2) signup_cal_sum_1,
        round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL' AND loan_type_name_ru='Sign up'   THEN loan_amount ELSE 0 END ),0),2) signup_cal_sum_2,
        round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL' AND loan_type_name_ru='Sign up'   THEN loan_amount ELSE 0 END ),0),2) signup_cal_sum_3,
        round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' AND loan_type_name_ru='Sign up'  THEN loan_amount ELSE 0 END ),0),2) signup_cal_sum_4,
        round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' AND loan_type_name_ru='Sign up (2 года)'  THEN loan_amount ELSE 0 END ),0),2) signup2_eff_sum_01,
        round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' AND loan_type_name_ru='Sign up (2 года)'  THEN loan_amount ELSE 0 END ),0),2) signup2_eff_sum_1,
        round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' AND loan_type_name_ru='Sign up (2 года)'  THEN loan_amount ELSE 0 END ),0),2) signup2_eff_sum_2,
        round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' AND loan_type_name_ru='Sign up (2 года)'  THEN loan_amount ELSE 0 END ),0),2) signup2_eff_sum_3,
        round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF' AND loan_type_name_ru='Sign up (2 года)'   THEN loan_amount ELSE 0 END ),0),2) signup2_eff_sum_4,
        round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL' AND loan_type_name_ru='Sign up (2 года)'   THEN loan_amount ELSE 0 END ),0),2) signup2_cal_sum_01,
        round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL' AND loan_type_name_ru='Sign up (2 года)'   THEN loan_amount ELSE 0 END ),0),2) signup2_cal_sum_1,
        round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL' AND loan_type_name_ru='Sign up (2 года)'   THEN loan_amount ELSE 0 END ),0),2) signup2_cal_sum_2,
        round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL' AND loan_type_name_ru='Sign up (2 года)'   THEN loan_amount ELSE 0 END ),0),2) signup2_cal_sum_3,
        round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' AND loan_type_name_ru='Sign up (2 года)'  THEN loan_amount ELSE 0 END ),0),2) signup2_cal_sum_4,
        round(nvl(sum(CASE WHEN year_nr=0 AND tp ='EFF' AND loan_type_name_ru='Retention'  THEN 0 ELSE 0 END ),0),2) retention_eff_sum_01,
        round(nvl(sum(CASE WHEN year_nr=1 AND tp ='EFF' AND loan_type_name_ru='Retention'  THEN loan_amount ELSE 0 END ),0),2) retention_eff_sum_1,
        round(nvl(sum(CASE WHEN year_nr=2 AND tp ='EFF' AND loan_type_name_ru='Retention'  THEN loan_amount ELSE 0 END ),0),2) retention_eff_sum_2,
        round(nvl(sum(CASE WHEN year_nr=3 AND tp ='EFF' AND loan_type_name_ru='Retention'  THEN loan_amount ELSE 0 END ),0),2) retention_eff_sum_3,
        round(nvl(sum(CASE WHEN year_nr=4 AND tp ='EFF' AND loan_type_name_ru='Retention'   THEN loan_amount ELSE 0 END ),0),2) retention_eff_sum_4,
        round(nvl(sum(CASE WHEN year_nr=0 AND tp ='CAL' AND loan_type_name_ru='Retention'   THEN 0 ELSE 0 END ),0),2) retention_cal_sum_01,
        round(nvl(sum(CASE WHEN year_nr=1 AND tp ='CAL' AND loan_type_name_ru='Retention'   THEN loan_amount ELSE 0 END ),0),2) retention_cal_sum_1,
        round(nvl(sum(CASE WHEN year_nr=2 AND tp ='CAL' AND loan_type_name_ru='Retention'   THEN loan_amount ELSE 0 END ),0),2) retention_cal_sum_2,
        round(nvl(sum(CASE WHEN year_nr=3 AND tp ='CAL' AND loan_type_name_ru='Retention'   THEN loan_amount ELSE 0 END ),0),2) retention_cal_sum_3,
        round(nvl(sum(CASE WHEN year_nr=4 AND tp ='CAL' AND loan_type_name_ru='Retention'  THEN loan_amount ELSE 0 END ),0),2) retention_cal_sum_4
  FROM
    (SELECT
       year_nr, login,loan_amount,'CAL' tp ,loan_type_name_ru FROM period, loan  s WHERE loan_contr_day_dt BETWEEN dfrom_cal AND dto_cal
    UNION ALL
    SELECT
      year_nr, login,loan_amount,'EFF' tp ,loan_type_name_ru FROM  period, loan  s WHERE  loan_contr_day_dt BETWEEN dfrom_eff AND dto_eff)
    GROUP BY login
)
 SELECT --основной запрос
         A.login
          ,A.assignment_id
          ,A.assignment_number
          ,s.universal_salary_amount asg_salary
          ,s.rep_currency_id asg_currency
          ,trunc(SYSDATE, 'month') - 1 report_date
          ,s.rep_salary_rate asg_rate
          ,A.legal_entity_name asg_legal_entity_name
          ,A.job_name asg_job_name
          ,A.contract_number asg_contract_number
          ,A.main_asg_flag
          ,A.person_id
          ,A.party_id
          ,sl.salary_amount salary
          ,sl.main_rep_currency_id main_asg_currency
          ,nvl(sn.salary_eff_sum_01,0) salary_eff_sum_01
          ,nvl(sn.salary_eff_sum_1,0) salary_eff_sum_1
          ,nvl(sn.salary_eff_sum_2,0) salary_eff_sum_2
          ,nvl(sn.salary_eff_sum_3,0) salary_eff_sum_3
          ,nvl(sn.salary_eff_sum_4,0) salary_eff_sum_4
          ,nvl(sn.salary_cal_sum_01,0) salary_cal_sum_01
          ,nvl(sn.salary_cal_sum_1,0) salary_cal_sum_1
          ,nvl(sn.salary_cal_sum_2,0) salary_cal_sum_2
          ,nvl(sn.salary_cal_sum_3,0) salary_cal_sum_3
          ,nvl(sn.salary_cal_sum_4,0) salary_cal_sum_4
          ,nvl(pn.bonus_eff_sum_01,0) bonus_eff_sum_01
          ,nvl(pn.bonus_eff_sum_1,0) bonus_eff_sum_1
          ,nvl(pn.bonus_eff_sum_2,0) bonus_eff_sum_2
          ,nvl(pn.bonus_eff_sum_3,0) bonus_eff_sum_3
          ,nvl(pn.bonus_eff_sum_4,0) bonus_eff_sum_4
		  ,nvl(pn.piecerate_eff_sum_01,0) piecerate_eff_sum_01
          ,nvl(pn.piecerate_eff_sum_1,0) piecerate_eff_sum_1
          ,nvl(pn.piecerate_eff_sum_2,0) piecerate_eff_sum_2
          ,nvl(pn.piecerate_eff_sum_3,0) piecerate_eff_sum_3
          ,nvl(pn.piecerate_eff_sum_4,0) piecerate_eff_sum_4
          ,nvl(pn.other_eff_sum_01,0) other_eff_sum_01
          ,nvl(pn.other_eff_sum_1,0) other_eff_sum_1
          ,nvl(pn.other_eff_sum_2,0) other_eff_sum_2
          ,nvl(pn.other_eff_sum_3,0) other_eff_sum_3
          ,nvl(pn.other_eff_sum_4,0) other_eff_sum_4
          ,nvl(pn.bonus_cal_sum_01,0) bonus_cal_sum_01
          ,nvl(pn.bonus_cal_sum_1,0) bonus_cal_sum_1
          ,nvl(pn.bonus_cal_sum_2,0) bonus_cal_sum_2
          ,nvl(pn.bonus_cal_sum_3,0) bonus_cal_sum_3
          ,nvl(pn.bonus_cal_sum_4,0) bonus_cal_sum_4
		  ,nvl(pn.piecerate_cal_sum_01,0) piecerate_cal_sum_01
          ,nvl(pn.piecerate_cal_sum_1,0) piecerate_cal_sum_1
          ,nvl(pn.piecerate_cal_sum_2,0) piecerate_cal_sum_2
          ,nvl(pn.piecerate_cal_sum_3,0) piecerate_cal_sum_3
          ,nvl(pn.piecerate_cal_sum_4,0) piecerate_cal_sum_4
          ,nvl(pn.other_cal_sum_01,0) other_cal_sum_01
          ,nvl(pn.other_cal_sum_1,0) other_cal_sum_1
          ,nvl(pn.other_cal_sum_2,0) other_cal_sum_2
          ,nvl(pn.other_cal_sum_3,0) other_cal_sum_3
          ,nvl(pn.other_cal_sum_4,0) other_cal_sum_4
          ,nvl(lyn.signup_eff_sum_01,0) signup_eff_sum_01
          ,nvl(lyn.signup_eff_sum_1,0) signup_eff_sum_1
          ,nvl(lyn.signup_eff_sum_2,0) signup_eff_sum_2
          ,nvl(lyn.signup_eff_sum_3,0) signup_eff_sum_3
          ,nvl(lyn.signup_eff_sum_4,0) signup_eff_sum_4
          ,nvl(lyn.signup_cal_sum_01,0) signup_cal_sum_01
          ,nvl(lyn.signup_cal_sum_1,0) signup_cal_sum_1
          ,nvl(lyn.signup_cal_sum_2,0) signup_cal_sum_2
          ,nvl(lyn.signup_cal_sum_3,0) signup_cal_sum_3
          ,nvl(lyn.signup_cal_sum_4,0) signup_cal_sum_4
          ,nvl(lyn.signup2_eff_sum_01,0) signup2_eff_sum_01
          ,nvl(lyn.signup2_eff_sum_1,0) signup2_eff_sum_1
          ,nvl(lyn.signup2_eff_sum_2,0) signup2_eff_sum_2
          ,nvl(lyn.signup2_eff_sum_3,0) signup2_eff_sum_3
          ,nvl(lyn.signup2_eff_sum_4,0) signup2_eff_sum_4
          ,nvl(lyn.signup2_cal_sum_01,0) signup2_cal_sum_01
          ,nvl(lyn.signup2_cal_sum_1,0) signup2_cal_sum_1
          ,nvl(lyn.signup2_cal_sum_2,0) signup2_cal_sum_2
          ,nvl(lyn.signup2_cal_sum_3,0) signup2_cal_sum_3
          ,nvl(lyn.signup2_cal_sum_4,0) signup2_cal_sum_4
          ,nvl(retention_eff_sum_01,0) retention_eff_sum_01
          ,nvl(lyn.retention_eff_sum_1,0) retention_eff_sum_1
          ,nvl(lyn.retention_eff_sum_2,0) retention_eff_sum_2
          ,nvl(lyn.retention_eff_sum_3,0) retention_eff_sum_3
          ,nvl(lyn.retention_eff_sum_4,0) retention_eff_sum_4
          ,nvl(lyn.retention_cal_sum_01,0) retention_cal_sum_01
          ,nvl(lyn.retention_cal_sum_1,0) retention_cal_sum_1
          ,nvl(lyn.retention_cal_sum_2,0) retention_cal_sum_2
          ,nvl(lyn.retention_cal_sum_3,0) retention_cal_sum_3
          ,nvl(lyn.retention_cal_sum_4,0) retention_cal_sum_4
          ,vn.vest_legal_entity_name
          ,nvl(vn.vest_le_eff_sum_01,0) vest_le_eff_sum_01
          ,nvl(vn.vest_le_eff_sum_1,0) vest_le_eff_sum_1
          ,nvl(vn.vest_le_eff_sum_2,0) vest_le_eff_sum_2
          ,nvl(vn.vest_le_eff_sum_3,0) vest_le_eff_sum_3
          ,nvl(vn.vest_le_eff_sum_4,0) vest_le_eff_sum_4
          ,nvl(vn.vest_le_cal_sum_01,0) vest_le_cal_sum_01
          ,nvl(vn.vest_le_cal_sum_1,0) vest_le_cal_sum_1
          ,nvl(vn.vest_le_cal_sum_2,0) vest_le_cal_sum_2
          ,nvl(vn.vest_le_cal_sum_3,0) vest_le_cal_sum_3
          ,nvl(vn.vest_le_cal_sum_4,0) vest_le_cal_sum_4
          ,nvl(vl.vest_eff_sum_01,0) vest_eff_sum_01
          ,nvl(vl.vest_eff_sum_1,0) vest_eff_sum_1
          ,nvl(vl.vest_eff_sum_2,0) vest_eff_sum_2
          ,nvl(vl.vest_eff_sum_3,0) vest_eff_sum_3
          ,nvl(vl.vest_eff_sum_4,0) vest_eff_sum_4
          ,nvl(vl.vest_cal_sum_01,0) vest_cal_sum_01
          ,nvl(vl.vest_cal_sum_1,0) vest_cal_sum_1
          ,nvl(vl.vest_cal_sum_2,0) vest_cal_sum_2
          ,nvl(vl.vest_cal_sum_3,0) vest_cal_sum_3
          ,nvl(vl.vest_cal_sum_4,0) vest_cal_sum_4
          , nvl(sn.salary_eff_sum_01,0)+nvl(pn.bonus_eff_sum_01,0)+nvl(pn.piecerate_eff_sum_01,0)+nvl(pn.other_eff_sum_01,0)+nvl(lyn.signup_eff_sum_01,0)+nvl(lyn.signup2_eff_sum_01,0)+nvl(lyn.retention_eff_sum_01,0)+nvl(vl.vest_eff_sum_01,0) income_eff_sum_01
          , nvl(sn.salary_eff_sum_1,0)+nvl(pn.bonus_eff_sum_1,0)+nvl(pn.piecerate_eff_sum_1,0)+nvl(pn.other_eff_sum_1,0)+nvl(lyn.signup_eff_sum_1,0)+nvl(lyn.signup2_eff_sum_1,0)+nvl(lyn.retention_eff_sum_1,0)+nvl(vl.vest_eff_sum_1,0) income_eff_sum_1
          , nvl(sn.salary_eff_sum_2,0)+nvl(pn.bonus_eff_sum_2,0)+nvl(pn.piecerate_eff_sum_2,0)+nvl(pn.other_eff_sum_2,0)+nvl(lyn.signup_eff_sum_2,0)+nvl(lyn.signup2_eff_sum_2,0)+nvl(lyn.retention_eff_sum_2,0)+nvl(vl.vest_eff_sum_2,0) income_eff_sum_2
          , nvl(sn.salary_eff_sum_3,0)+nvl(pn.bonus_eff_sum_3,0)+nvl(pn.piecerate_eff_sum_3,0)+nvl(pn.other_eff_sum_3,0)+nvl(lyn.signup_eff_sum_3,0)+nvl(lyn.signup2_eff_sum_3,0)+nvl(lyn.retention_eff_sum_3,0)+nvl(vl.vest_eff_sum_3,0) income_eff_sum_3
          , nvl(sn.salary_eff_sum_4,0)+nvl(pn.bonus_eff_sum_4,0)+nvl(pn.piecerate_eff_sum_4,0)+nvl(pn.other_eff_sum_4,0)+nvl(lyn.signup_eff_sum_4,0)+nvl(lyn.signup2_eff_sum_4,0)+nvl(lyn.retention_eff_sum_4,0)+nvl(vl.vest_eff_sum_4,0) income_eff_sum_4
          , nvl(sn.salary_cal_sum_01,0)+nvl(pn.bonus_cal_sum_01,0)+nvl(pn.piecerate_cal_sum_01,0)+nvl(pn.other_cal_sum_01,0)+nvl(lyn.signup_cal_sum_01,0)+nvl(lyn.signup2_cal_sum_01,0)+nvl(lyn.retention_cal_sum_01,0)+nvl(vl.vest_cal_sum_01,0) income_cal_sum_01
          , nvl(sn.salary_cal_sum_1,0)+nvl(pn.bonus_cal_sum_1,0)+nvl(pn.piecerate_cal_sum_1,0)+nvl(pn.other_cal_sum_1,0)+nvl(lyn.signup_cal_sum_1,0)+nvl(lyn.signup2_cal_sum_1,0)+nvl(lyn.retention_cal_sum_1,0)+nvl(vl.vest_cal_sum_1,0) income_cal_sum_1
          , nvl(sn.salary_cal_sum_2,0)+nvl(pn.bonus_cal_sum_2,0)+nvl(pn.piecerate_cal_sum_2,0)+nvl(pn.other_cal_sum_2,0)+nvl(lyn.signup_cal_sum_2,0)+nvl(lyn.signup2_cal_sum_2,0)+nvl(lyn.retention_cal_sum_2,0)+nvl(vl.vest_cal_sum_2,0) income_cal_sum_2
          , nvl(sn.salary_cal_sum_3,0)+nvl(pn.bonus_cal_sum_3,0)+nvl(pn.piecerate_cal_sum_3,0)+nvl(pn.other_cal_sum_3,0)+nvl(lyn.signup_cal_sum_3,0)+nvl(lyn.signup2_cal_sum_3,0)+nvl(lyn.retention_cal_sum_3,0)+nvl(vl.vest_cal_sum_3,0) income_cal_sum_3
          , nvl(sn.salary_cal_sum_4,0)+nvl(pn.bonus_cal_sum_4,0)+nvl(pn.piecerate_cal_sum_4,0)+nvl(pn.other_cal_sum_4,0)+nvl(lyn.signup_cal_sum_4,0)+nvl(lyn.signup2_cal_sum_4,0)+nvl(lyn.retention_cal_sum_4,0)+nvl(vl.vest_cal_sum_4,0) income_cal_sum_4
          , p.scal_01
          , p.scal_1
          , p.scal_2
          , p.scal_3
          , p.scal_4
          , p.seff_beg_01
          , p.seff_beg_1
          , p.seff_beg_2
          , p.seff_beg_3
          , p.seff_beg_4
          , p.seff_end_01
          , p.seff_end_1
          , p.seff_end_2
          , p.seff_end_3
          , p.seff_end_4
    FROM  period_year_nr p ,
   assignments A
  LEFT JOIN salary s ON A.assignment_id = s.assignment_id
  LEFT JOIN salary_year_nr sn ON A.login=sn.login
  LEFT JOIN payment_year_nr pn ON A.login=pn.login
  LEFT JOIN salary_login sl ON A.login=sl.login
  LEFT JOIN vesting_year_nr vn ON A.login=vn.login
  LEFT JOIN vesting_login_year_nr vl ON A.login=vl.login
  LEFT JOIN loan_year_nr lyn ON A.login=lyn.login
;
comment on table BIHR.REP_STAFFCABINET_V is 'Данные для финкабинета STAFF';
comment on column BIHR.REP_STAFFCABINET_V.LOGIN is 'Логин сотрудника';
comment on column BIHR.REP_STAFFCABINET_V.ASSIGNMENT_ID is 'ID назначения';
comment on column BIHR.REP_STAFFCABINET_V.ASSIGNMENT_NUMBER is 'Номер назначения';
comment on column BIHR.REP_STAFFCABINET_V.ASG_SALARY is 'Оклад назначения';
comment on column BIHR.REP_STAFFCABINET_V.ASG_CURRENCY is 'Валюта назначения';
comment on column BIHR.REP_STAFFCABINET_V.REPORT_DATE is 'Дата актуальности данных';
comment on column BIHR.REP_STAFFCABINET_V.ASG_RATE is 'Ставка';
comment on column BIHR.REP_STAFFCABINET_V.ASG_LEGAL_ENTITY_NAME is 'Юрлицо назначения';
comment on column BIHR.REP_STAFFCABINET_V.ASG_JOB_NAME is 'Должность';
comment on column BIHR.REP_STAFFCABINET_V.ASG_CONTRACT_NUMBER is 'Номер трудового договора';
comment on column BIHR.REP_STAFFCABINET_V.MAIN_ASG_FLAG is 'Основное назначение';
comment on column BIHR.REP_STAFFCABINET_V.PERSON_ID is 'ID персоны';
comment on column BIHR.REP_STAFFCABINET_V.PARTY_ID is 'PartyID';
comment on column BIHR.REP_STAFFCABINET_V.SALARY is 'Текущий оклад';
comment on column BIHR.REP_STAFFCABINET_V.MAIN_ASG_CURRENCY is 'Валюта основного назначения';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_EFF_SUM_01 is 'Суммарный оклад от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_EFF_SUM_1 is 'Суммарный оклад от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_EFF_SUM_2 is 'Суммарный оклад от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_EFF_SUM_3 is 'Суммарный оклад от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_EFF_SUM_4 is 'Суммарный оклад от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_CAL_SUM_01 is 'Суммарный оклад за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_CAL_SUM_1 is 'Суммарный оклад за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_CAL_SUM_2 is 'Суммарный оклад за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_CAL_SUM_3 is 'Суммарный оклад за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.SALARY_CAL_SUM_4 is 'Суммарный оклад за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_EFF_SUM_01 is 'Сумма премиальных выплат от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_EFF_SUM_1 is 'Сумма премиальных выплат от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_EFF_SUM_2 is 'Сумма премиальных выплат от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_EFF_SUM_3 is 'Сумма премиальных выплат от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_EFF_SUM_4 is 'Сумма премиальных выплат от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_EFF_SUM_01 is 'Сдельные выплаты от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_EFF_SUM_1 is 'Сдельные выплаты от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_EFF_SUM_2 is 'Сдельные выплаты от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_EFF_SUM_3 is 'Сдельные выплаты от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_EFF_SUM_4 is 'Сдельные выплаты от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_EFF_SUM_01 is 'Сумма других выплат от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_EFF_SUM_1 is 'Сумма других выплат от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_EFF_SUM_2 is 'Сумма других выплат от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_EFF_SUM_3 is 'Сумма других выплат от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_EFF_SUM_4 is 'Сумма других выплат от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_CAL_SUM_01 is 'Сумма премиальных выплат за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_CAL_SUM_1 is 'Сумма премиальных выплат за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_CAL_SUM_2 is 'Сумма премиальных выплат за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_CAL_SUM_3 is 'Сумма премиальных выплат за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.BONUS_CAL_SUM_4 is 'Сумма премиальных выплат за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_CAL_SUM_01 is 'Сдельные выплаты за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_CAL_SUM_1 is 'Сдельные выплаты за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_CAL_SUM_2 is 'Сдельные выплаты за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_CAL_SUM_3 is 'Сдельные выплаты за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.PIECERATE_CAL_SUM_4 is 'Сдельные выплаты за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_CAL_SUM_01 is 'Сумма других выплат за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_CAL_SUM_1 is 'Сумма других выплат за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_CAL_SUM_2 is 'Сумма других выплат за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_CAL_SUM_3 is 'Сумма других выплат за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.OTHER_CAL_SUM_4 is 'Сумма других выплат за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_EFF_SUM_01 is 'Sing up (Если есть активный) от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_EFF_SUM_1 is 'Sing up (Если есть активный) от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_EFF_SUM_2 is 'Sing up (Если есть активный) от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_EFF_SUM_3 is 'Sing up (Если есть активный) от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_EFF_SUM_4 is 'Sing up (Если есть активный) от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_CAL_SUM_01 is 'Sing up (Если есть активный) за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_CAL_SUM_1 is 'Sing up (Если есть активный) за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_CAL_SUM_2 is 'Sing up (Если есть активный) за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_CAL_SUM_3 is 'Sing up (Если есть активный) за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP_CAL_SUM_4 is 'Sing up (Если есть активный) за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_EFF_SUM_01 is 'Sing up 2 года (Если есть активный) от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_EFF_SUM_1 is 'Sing up 2 года (Если есть активный) от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_EFF_SUM_2 is 'Sing up 2 года (Если есть активный) от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_EFF_SUM_3 is 'Sing up 2 года (Если есть активный) от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_EFF_SUM_4 is 'Sing up 2 года (Если есть активный) от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_CAL_SUM_01 is 'Sing up 2 года (Если есть активный) за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_CAL_SUM_1 is 'Sing up 2 года (Если есть активный) за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_CAL_SUM_2 is 'Sing up 2 года (Если есть активный) за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_CAL_SUM_3 is 'Sing up 2 года (Если есть активный) за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.SIGNUP2_CAL_SUM_4 is 'Sing up 2 года (Если есть активный) за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_EFF_SUM_01 is 'Retention (Если есть активный) от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_EFF_SUM_1 is 'Retention (Если есть активный) от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_EFF_SUM_2 is 'Retention (Если есть активный) от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_EFF_SUM_3 is 'Retention (Если есть активный) от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_EFF_SUM_4 is 'Retention (Если есть активный) от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_CAL_SUM_01 is 'Retention (Если есть активный) за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_CAL_SUM_1 is 'Retention (Если есть активный) за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_CAL_SUM_2 is 'Retention (Если есть активный) за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_CAL_SUM_3 is 'Retention (Если есть активный) за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.RETENTION_CAL_SUM_4 is 'Retention (Если есть активный) за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LEGAL_ENTITY_NAME is 'Юрлицо (опционы)';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_EFF_SUM_01 is 'Сумма завестившейся части от даты расчета за +1год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_EFF_SUM_1 is 'Сумма завестившейся части от даты расчета за -1год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_EFF_SUM_2 is 'Сумма завестившейся части от даты расчета за -2год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_EFF_SUM_3 is 'Сумма завестившейся части от даты расчета за -3год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_EFF_SUM_4 is 'Сумма завестившейся части от даты расчета за -4год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_CAL_SUM_01 is 'Сумма завестившейся части за календарный +1год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_CAL_SUM_1 is 'Сумма завестившейся части за календарный -1год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_CAL_SUM_2 is 'Сумма завестившейся части за календарный -2год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_CAL_SUM_3 is 'Сумма завестившейся части за календарный -3год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_LE_CAL_SUM_4 is 'Сумма завестившейся части за календарный -4год по ЮЛ';
comment on column BIHR.REP_STAFFCABINET_V.VEST_EFF_SUM_01 is 'Сумма завестившейся части от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_EFF_SUM_1 is 'Сумма завестившейся части от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_EFF_SUM_2 is 'Сумма завестившейся части от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_EFF_SUM_3 is 'Сумма завестившейся части от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_EFF_SUM_4 is 'Сумма завестившейся части от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_CAL_SUM_01 is 'Сумма завестившейся части за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_CAL_SUM_1 is 'Сумма завестившейся части за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_CAL_SUM_2 is 'Сумма завестившейся части за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_CAL_SUM_3 is 'Сумма завестившейся части за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.VEST_CAL_SUM_4 is 'Сумма завестившейся части за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_EFF_SUM_01 is 'Совокупный доход от даты расчета за +1год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_EFF_SUM_1 is 'Совокупный доход от даты расчета за -1год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_EFF_SUM_2 is 'Совокупный доход от даты расчета за -2год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_EFF_SUM_3 is 'Совокупный доход от даты расчета за -3год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_EFF_SUM_4 is 'Совокупный доход от даты расчета за -4год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_CAL_SUM_01 is 'Совокупный доход за календарный +1год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_CAL_SUM_1 is 'Совокупный доход за календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_CAL_SUM_2 is 'Совокупный доход за календарный -2год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_CAL_SUM_3 is 'Совокупный доход за календарный -3год';
comment on column BIHR.REP_STAFFCABINET_V.INCOME_CAL_SUM_4 is 'Совокупный доход за календарный -4год';
comment on column BIHR.REP_STAFFCABINET_V.SCAL_01 is 'Календарный период +1год';
comment on column BIHR.REP_STAFFCABINET_V.SCAL_1 is 'Период Календарный -1год';
comment on column BIHR.REP_STAFFCABINET_V.SCAL_2 is 'Период Календарный -2года';
comment on column BIHR.REP_STAFFCABINET_V.SCAL_3 is 'Период Календарный -3года';
comment on column BIHR.REP_STAFFCABINET_V.SCAL_4 is 'Период Календарный -4года';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_BEG_01 is '+1год от даты расчета начало периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_BEG_1 is '1 год от даты расчета начало периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_BEG_2 is '2 года от даты расчета начало периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_BEG_3 is '3 года от даты расчета начало периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_BEG_4 is '4 года от даты расчета начало периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_END_01 is '+1год от даты расчета конец периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_END_1 is '1 год от даты расчета конец периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_END_2 is '2 года от даты расчета конец периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_END_3 is '3 года от даты расчета конец периода';
comment on column BIHR.REP_STAFFCABINET_V.SEFF_END_4 is '4 года от даты расчета конец периода';
