CREATE OR REPLACE VIEW BIHR.REP_HEADCOUNT_INFO_V AS
WITH report_params AS
(
  SELECT
         param1
    FROM report_parameters_tbl
   WHERE report_name = 'REP_HEADCOUNT_MOVING'
     AND ROWNUM = 1
),
 GL_PERIOD as
(
        select mth_start_dt start_date
              ,mth_end_dt end_date
              ,to_char(mth_start_dt,
                       'Mon-yy',
                       'NLS_DATE_LANGUAGE = AMERICAN') period_name
        from OEBS_GL_PERIOD_STATUSES
            ,lu_dt_month
            ,report_params
        where mth_start_dt = start_date and period_name =param1
        group by mth_start_dt
                ,mth_end_dt
)
,HEADCOUNT_MOVING_GK as (
SELECT A.ae_header_id as GL_ae_header_id
  , A.ae_line_num as GL_ae_line_num --id заголовка журнала --id строки журнала
  , A.assignment_id as GL_assignment_id
  , A.position_id as GL_position_id
  , A.ledger_id as GL_ledger_id
  , p.start_date as period_start_date
  , A.period_to_date as gl_actual_headcount --численность в разрезе аналитик
  , decode(A.application_id, 801,'HR', 101, 'MANUAL') as GL_source_descr --источник данных
  , B.segment1, B.Company_Name as segment1_name
  , B.segment2, B.Account_Name as segment2_name
  , B.segment3, B.Analytics_Name as segment3_name
  , B.segment4, B.Hype_Cfo_Name as segment4_name
  , B.segment5, B.mvp_name as segment5_name
  , B.segment6, B.program_name as segment6_name
  , B.segment7, B.Service_Name as segment7_name
  , B.segment8, B.Sales_Name as segment8_name
  , B.segment9, B.Interco_Name as segment9_name
  , B.segment10, B.tax_name as segment10_name
  , B.segment11, B.Reserve1_Name as segment11_name
  , B.segment12, B.Reserve2_Name as segment12_name
  ,p.period_name
 FROM
 (select * from oebs_xxya_all_sla_store_tbl A  where  A.currency = 'STAT1' AND A.balance_type_code = 'A' AND  A.application_id IN (801, 101)) A --HR,MANUAL
 JOIN GL_PERIOD p ON p.period_name=A.period_name_gl
 JOIN lu_code_combination B ON  A.ccid = B.code_combination_id  AND B.segment2 IN ('XSTATHEADE','XSTATHEADP')
 JOIN lu_ledger LL ON  LL.ledger_id = a.ledger_id AND LL.gaap_flag = 1
)
 ,period_charges_info as (
SELECT
       CH.position_id as CH_position_id
      ,CH.assignment_id as CH_assignment_id
      ,CH.ae_header_id as CH_ae_header_id
      ,CH.ae_line_num as CH_ae_line_num
      ,CH.ledger_id as CH_ledger_id
      ,CH.period_start_date as CH_period_start_date
      ,CH.period_end_date as CH_period_end_date
      ,i.code_combination_id as CH_code_combination_id
      ,i.num_info3 as CH_assignment_status
      ,'WAREH' as CH_source_descr
      ,CH.position_id --Наименование БП
      ,I.char_info1 AS bp_list --склеенный список БП для актуальных назначений для id сотрудника (party_id)
      ,I.char_info2 AS payment_type --Тип оплаты
      ,I.year_ -- Год,
      ,I.num_info1 --Расположение с назначения
      ,I.num_info9 --Грейд
      ,I.num_info10 -- Должность с назначения
      ,I.num_info6 as product_value -- Продукт (product_value)
      ,I.num_info11 as assign_staff_rate -- Ставка кадровая с назначения (из бюджетных значений)
      ,I.num_info2 --Иерархия Подразделений
  --  , POSSTAT.bp_status_code --состояние БП
     ,I.num_info12
     ,i.concatenated_segments
     ,i.num_info8
     ,i.num_info4
     ,p.period_name
FROM oebs_xxbud_period_charges_info I
JOIN GL_PERIOD  p ON I.period_start_date  between p.start_date and p.end_date
JOIN oebs_xxbud_period_charges CH ON
                                         CH.period_start_date =
                                          I.period_start_date AND
                                          CH.period_end_date =
                                          I.period_end_date AND
                                          I.code_combination_id =
                                          CH.code_combination_id AND
                                          I.assignment_id =
                                          CH.assignment_id AND
                                          NVL(CH.position_id, -99) =
                                          nvl(I.position_id, -99)  and I.charge_type = 'HCFACT' AND I.row_status = 'XLA' AND    CH.charge_type = 'HCFACT' AND CH.row_status = 'XLA'
  --LEFT JOIN oebs_xxhr_bi_bp_status_tbl POSSTAT ON POSSTAT.position_id = CH.position_id AND POSSTAT.Assignment_Id = CH.assignment_id
  -- AND CH.period_end_date BETWEEN NVL(POSSTAT.date_from, BICOMMON_API_PKG.minimum_date) AND NVL(POSSTAT.date_to, BICOMMON_API_PKG.maximum_date)
)
,GL_SH AS(
select
      NVL(GL.gl_assignment_id, CH_I.ch_assignment_id) assignment_id
      ,NVL(CH_period_start_date,gl.period_start_date) as period_start_date --служебные
      ,GL.segment1
      ,GL.segment1_name
      ,GL.segment2
      ,GL.segment2_name
      ,GL.segment3
      ,GL.segment3_name
      ,GL.segment4
      ,GL.segment4_name
      ,GL.segment5
      ,GL.segment5_name
      ,GL.segment6
      ,GL.segment6_name
      ,GL.segment7
      ,GL.segment7_name
      ,GL.segment8
      ,GL.segment8_name
      ,GL.segment9
      ,GL.segment9_name
      ,GL.segment10
      ,GL.segment10_name
      ,GL.segment11
      ,GL.segment11_name
      ,GL.segment12
      , GL.segment12_name
      ,gl_actual_headcount
   --   ,CH_ae_header_id
    --  ,CH_ae_line_num
      ,CH_period_end_date
      ,CH_code_combination_id
      ,CH_assignment_status
     --,CH_I.position_id --Наименование БП
      ,CH_I.bp_list --склеенный список БП для актуальных назначений для id сотрудника (party_id)
      ,CH_I.payment_type --Тип оплаты
   --   ,CH_I.year_ -- Год,
      ,CH_I.num_info1 --Расположение с назначения
      ,CH_I.num_info9 --Грейд
      ,CH_I.num_info10 -- Должность с назначения
     ,CH_I.product_value -- Продукт (product_value)
        ,CH_I.assign_staff_rate -- Ставка кадровая с назначения (из бюджетных значений)
      ,CH_I.num_info2 --Иерархия Подразделений
   --   ,CH_I.bp_status_code --состояние БП
       ,nvl(CH_position_id, GL_position_id) as position_id
      ,nvl(CH_ledger_id, GL_ledger_id) as ledger_id
      ,nvl(GL_source_descr, CH_source_descr) as source_descr --источник данных
      ,case --признак проводки
         when CH_I.ch_ae_header_id is not null and
              CH_I.ch_assignment_id is null then
          'невалидная запись в ХД' ---1 --невалидная запись в ХД (есть запись в ХД с пустым assignment_id)
         when CH_I.ch_ae_header_id is null then
          'ручная корректировка' --0 --ручная (нет записи в ХД)
         when CH_I.ch_assignment_id is not null then
          'переданная численность' --1 --переданная (есть запись в ХД)
       end as source_sign
       ,CH_I.num_info12
       ,CH_I.concatenated_segments
       ,CH_I.num_info8
       ,CH_I.num_info4
       ,nvl( GL.period_name, CH_I.period_name) period_name
FROM --"GL" --главная книга и численность в проводках
    HEADCOUNT_MOVING_GK  GL--gl_actual_headcount
FULL OUTER JOIN --CH_I: "CHARGES" + "CHARGES_INFO" --проводки в Хранилище данных, которые соединяем с проводками из ГК (если это возможно)
period_charges_info CH_I ON CH_I.CH_period_start_date =
                            gl.period_start_date AND
                            CH_I.CH_ae_header_id =GL.GL_ae_header_id AND
                            CH_I.CH_ae_line_num =GL.GL_ae_line_num AND
                            CH_I.CH_ledger_id =GL.GL_ledger_id AND
                           GL.gl_assignment_id =CH_I.CH_ASSIGNMENT_ID
                        )
, GL_SH_COUNT as(
select
       PERIOD_START_DATE
        ,ASSIGNMENT_ID
        ,SEGMENT1
        ,SEGMENT1_NAME
        ,SEGMENT2
        ,SEGMENT2_NAME
        ,SEGMENT3
        ,SEGMENT3_NAME
        ,SEGMENT4
        ,SEGMENT4_NAME
        ,SEGMENT5
        ,SEGMENT5_NAME
        ,SEGMENT6
        ,SEGMENT6_NAME
        ,SEGMENT7
        ,SEGMENT7_NAME
        ,SEGMENT8
        ,SEGMENT8_NAME
        ,SEGMENT9
        ,SEGMENT9_NAME
        ,SEGMENT10
        ,SEGMENT10_NAME
        ,SEGMENT11
        ,SEGMENT11_NAME
        ,SEGMENT12
        ,SEGMENT12_NAME
    --    ,CH_AE_HEADER_ID
        ,CH_PERIOD_END_DATE
        ,CH_CODE_COMBINATION_ID
        ,CH_ASSIGNMENT_STATUS
        ,BP_LIST
        ,PAYMENT_TYPE
        ,NUM_INFO1
        ,NUM_INFO9
        ,NUM_INFO10
        ,product_value
        ,ASSIGN_STAFF_RATE
        ,NUM_INFO2
     --   ,BP_STATUS_CODE
        ,POSITION_ID
        ,LEDGER_ID
        ,SOURCE_DESCR
        ,SOURCE_SIGN
        , sum(nvl(gl_actual_headcount,0)) as gl_actual_headcount
        ,num_info12
       ,concatenated_segments
       ,num_info8
       ,num_info4
       ,period_name
from GL_SH
group by PERIOD_START_DATE
        ,ASSIGNMENT_ID
        ,SEGMENT1
        ,SEGMENT1_NAME
        ,SEGMENT2
        ,SEGMENT2_NAME
        ,SEGMENT3
        ,SEGMENT3_NAME
        ,SEGMENT4
        ,SEGMENT4_NAME
        ,SEGMENT5
        ,SEGMENT5_NAME
        ,SEGMENT6
        ,SEGMENT6_NAME
        ,SEGMENT7
        ,SEGMENT7_NAME
        ,SEGMENT8
        ,SEGMENT8_NAME
        ,SEGMENT9
        ,SEGMENT9_NAME
        ,SEGMENT10
        ,SEGMENT10_NAME
        ,SEGMENT11
        ,SEGMENT11_NAME
        ,SEGMENT12
        ,SEGMENT12_NAME
   --     ,CH_AE_HEADER_ID
        ,CH_PERIOD_END_DATE
        ,CH_CODE_COMBINATION_ID
        ,CH_ASSIGNMENT_STATUS
        ,BP_LIST
        ,PAYMENT_TYPE
        ,NUM_INFO1
        ,NUM_INFO9
        ,NUM_INFO10
        ,product_value
        ,ASSIGN_STAFF_RATE
        ,NUM_INFO2
   --     ,BP_STATUS_CODE
        ,POSITION_ID
        ,LEDGER_ID
        ,SOURCE_DESCR
        ,SOURCE_SIGN
        ,num_info12
        ,concatenated_segments
       ,num_info8
       ,num_info4
       ,period_name
        )
  , xxhr_assignments as (
       SELECT ASS.assignment_id
                 ,MIN(ASS.party_id) as party_id
                 ,MIN(ASS.login) as login
                 ,nvl(sum( INSTR(UPPER(BPA.bp_status_code) , 'REPLACEMENT') ),0 )  REPLACEMENT
           FROM oebs_xxhr_assignments ASS
            JOIN GL_PERIOD LDT_
                 ON LDT_.Start_Date<=trunc(ASS.effective_end_date, 'mm')   AND  LDT_.End_Date>=trunc(ASS.effective_start_date, 'mm')
          LEFT JOIN f_bp_analytics_v BPA --чтобы вытащить 'new','replacement'
                    ON BPA.position_id = ASS.position_id
                  AND BPA.assignment_id = ASS.assignment_id
                  AND last_day(LDT_.Start_Date) BETWEEN BPA.date_from AND BPA.date_to
           GROUP BY ASS.assignment_id
                 )
      select
     LDT.mth_id
     ,upper(LDT.mth_desc_us) as mth_desc_us
      ,LDT.mth_end_dt
      ,a.Party_id
      ,a.LOGIN
      ,p.full_name
      ,fi.PERIOD_START_DATE
      ,fi.ASSIGNMENT_ID
      ,poss.name position_name --Наименование БП
      ,SUBSTR(POSS.name, INSTR(POSS.name, '.', -1) + 1) position_num --номер БП
      ,POSSTAT.bp_status_code --состояние БП,
      ,EC.category_short_name_ru AS payment_type_name --Тип оплаты (наименование)
      ,LDT.year_desc year_
      ,ll.location_name_ru
      ,g.position_grade_level
      ,substr(G.position_grade_name_ru,
              1,
              instr(position_grade_name_ru, '.', -1) - 1) profession --Профессия
      ,JB.attribute1 as assign_job_name -- Должность с назначения
      ,PB.name as assign_pay_basis_name -- Базис оклада с назначения
      ,fi.assign_staff_rate -- Ставка кадровая с назначения (из бюджетных значений)
      ,fi.product_value -- Продукт (product_value)
      ,PR.position_product_name_ru as product_name -- Продукт (наименование)
      ,LDH.Leaf_Dep_Name -- Бюджетное подразделение с БП
      ,LDDD.dep_name as dep_name_l2 --Отдел
      ,LDH.Department_Concat_Hier --Иерархия Подразделений
      ,segment1
      ,segment1_name
      ,segment2
      ,segment2_name
      ,segment3
      ,segment3_name
      ,segment4
      ,segment4_name
      ,segment5
      ,segment5_name
      ,segment6
      ,segment6_name
      ,segment7
      ,segment7_name
      ,segment8
      ,segment8_name
      ,segment9
      ,segment9_name
      ,segment10
      ,segment10_name
      ,segment11
      ,segment11_name
      ,segment12
      ,segment12_name
      ,fi.gl_actual_headcount as actual_headcount
      ,BOH.bo_concat_hier
      ,bp_list
      ,nvl(regexp_substr(BOH.level10_id, '[^~]+'), '-1') bo_id
      ,BOH.level10_name
      ,BOH.level9_name
      ,BOH.level8_name
      ,BOH.level7_name
      ,BOH.level6_name
      ,BOH.level5_name
      ,BOH.level4_name
      ,BOH.level3_name
      ,BOH.level2_name
      ,BOH.level1_name
      ,BOHA.bo_concat_hier as bo_concat_hier_altern
     ,nvl(regexp_substr(BOHA.level10_id, '[^~]+'), '-1') bo_id_altern
      ,BOHA.level10_name as level10_name_altern
      ,BOHA.level9_name as level9_name_altern
      ,BOHA.level8_name as level8_name_altern
      ,BOHA.level7_name as level7_name_altern
      ,BOHA.level6_name as level6_name_altern
      ,BOHA.level5_name as level5_name_altern
      ,BOHA.level4_name as level4_name_altern
      ,BOHA.level3_name as level3_name_altern
      ,BOHA.level2_name as level2_name_altern
      ,BOHA.level1_name as level1_name_altern
      ,source_sign data_type
        --is_maternity
         , CASE WHEN
   (fi.CH_assignment_status = 2099) OR
   (fi.CH_assignment_status = 1 AND fi.num_info8 = 1 AND fi.assign_staff_rate <= 0.4 --акт.назнач., матерн., кадр.ставка
    AND  --3й по порядку сегмент = 'HOME' (сегменты между собой разделены точками)
     SUBSTR(UPPER(fi.concatenated_segments), regexp_instr(fi.concatenated_segments, '\.' , 1 , 2 )+1, --pattern, start_position, nth_appearance
            regexp_instr(fi.concatenated_segments, '\.' , 1 , 3 ) - regexp_instr(fi.concatenated_segments, '\.' , 1 , 2 )-1
            ) = 'HOME'
    ) THEN 1 ELSE 0
    END as is_maternity
      --IS_INTERN
  , CASE WHEN fi.num_info4 = 0 --численность с бп
    AND UPPER(REPLACE(JB.name,'Ё','Е')) LIKE '%СТАЖЕР%'
    AND UPPER(PB.name) NOT LIKE '%СДЕЛЬНАЯ%ОПЛАТА%'
    AND decode(G.position_grade_level, 'Без грейда', 0, G.position_grade_level) <= 13
  THEN 1 ELSE 0
    END as is_intern --стажер
      --IS_OUTSTAFF
  , CASE WHEN  fi.num_info4 = 0 --численность с бп
   AND  instr('~'||LDH.department_hier_id||'~' , '~89201~') >0 --признак "Аутстафф" (цепочка будущих переименований организации)
   THEN 1 ELSE 0
    END as is_outstaff --/признак "Аутстафф"
   ,CASE  WHEN (';'||CH_assignment_status||';' LIKE '%;1;%' OR ';'||CH_assignment_status||';' LIKE '%;2099;%'  OR  ';'||CH_assignment_status||';' LIKE '%;8103;%') THEN 1 ELSE 0 END   status
        ,fi.position_id
      , fi.payment_type
      , fi.CH_assignment_status
      , fi.num_info1
      , fi.num_info9
      , fi.num_info10
      , fi.num_info2
      ,sysdate insert_date
      ,period_name
           ,' ' cause
           ,null gl_actual_headcnt_delta
           ,REPLACEMENT
               , fi.position_id||POSSTAT.bp_status_code||fi.payment_type||fi.CH_assignment_status||
fi.num_info1||fi.num_info2||fi.num_info9||fi.num_info10||
PB.name||fi.assign_staff_rate||fi.product_value||
fi.segment1||fi.segment2||fi.segment3||fi.segment4||fi.segment5||fi.segment6||fi.segment7||fi.segment8||fi.segment9||fi.segment10||fi.segment12 concat_key
from GL_SH_COUNT fi
JOIN lu_dt_month LDT on period_start_date = LDT.Mth_Start_Dt
LEFT JOIN  xxhr_assignments a on fi.assignment_id =a.assignment_id
LEFT JOIN oebs_per_assignments ASS_PER ON ASS_PER.assignment_id =
                                               fi.assignment_id and
                                               LDT.mth_end_dt BETWEEN
                                               ASS_PER.EFFECTIVE_START_DATE and
                                               ASS_PER.EFFECTIVE_END_DATE
LEFT JOIN oebs_per_people P ON P.person_id = ASS_PER.person_id and
                                    LDT.mth_end_dt BETWEEN
                                    P.EFFECTIVE_START_DATE and
                                    P.EFFECTIVE_END_DATE
LEFT JOIN oebs_hr_all_positions_f POSS ON fi.position_id =
                                               POSS.position_id and
                                               LDT.mth_end_dt BETWEEN
                                               POSS.effective_start_date and
                                               POSS.effective_end_date
LEFT JOIN oebs_xxhr_bi_bp_status_tbl POSSTAT ON POSS.position_id =
                                                     POSSTAT.position_id and
                           POSSTAT.assignment_id=fi.assignment_id and
                                                     LDT.mth_end_dt BETWEEN
                                                     POSSTAT.date_from AND
                                                     nvl(POSSTAT.date_to,
                                                         BICOMMON_API_PKG.maximum_date)
LEFT JOIN lu_employee_category EC ON fi.payment_type =
                                          EC.employee_category_id
LEFT JOIN Lu_Location LL ON LL.location_id = fi.num_info1 AND
                                 LDT.mth_end_dt BETWEEN LL.from_day_dt AND
                                 LL.to_day_dt
LEFT JOIN lu_bp_product PR ON PR.position_product_id =
                                   fi.product_value
  LEFT JOIN lu_department_hier LDH ON fi.num_info2 = LDH.leaf_dep_id AND
                                       LDT.mth_end_dt  BETWEEN LDH.from_day_dt AND  nvl(LDH.to_day_dt,  LDT.mth_end_dt+1)
LEFT JOIN lu_department LDDD ON LDH.a_dep_info_id = LDDD.dep_info_id and LDT.mth_end_dt BETWEEN LDDD.from_day_dt AND  nvl(LDDD.to_day_dt,  LDT.mth_end_dt+1)
LEFT JOIN f_budget_position_info BPI ON BPI.position_id =
                                             fi.position_id AND
                                             LDT.mth_end_dt BETWEEN
                                             BPI.from_day_dt AND
                                             BPI.to_day_dt
LEFT JOIN lu_bp_budget_org_hier BOH ON BOH.budget_org_hier_id =
                                            BPI.budget_org_hier_id
LEFT JOIN lu_bp_budget_org_hier_altern BOHA ON BOHA.budget_org_hier_id =
                                                    BPI.budget_org_hier_altern_id
   LEFT JOIN lu_bp_grade G ON G.position_grade_id = fi.num_info9
   LEFT JOIN oebs_per_jobs JB ON JB.job_id = fi.num_info10 AND
                                 LDT.mth_end_dt  BETWEEN JB.date_from AND   nvl(JB.date_to, LDT.mth_end_dt+1)
   LEFT JOIN oebs_per_pay_bases PB ON PB.pay_basis_id = fi.num_info12
;
