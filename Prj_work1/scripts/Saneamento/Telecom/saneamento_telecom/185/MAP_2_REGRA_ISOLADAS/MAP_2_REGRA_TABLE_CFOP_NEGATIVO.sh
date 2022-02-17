#!/bin/bash
echo ${TABELA_CONTROLE} ${TABELA_CFOP_NEGATIVO}
sqlplus -S /nolog <<@EOF >> ${SPOOL_FILE}.log 2>> ${SPOOL_FILE}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_TABLE_CFOP_NEGATIVO'
var exit_code             NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_TABLE_CFOP_NEGATIVO  ${TABELA_CONTROLE} ${TABELA_CFOP_NEGATIVO}
PROMPT ### Inicio ###
PROMPT


ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_ini_proc      = SYSDATE,
	   cp.st_processamento = 'Drop Table',
       cp.ds_msg_erro      = substr('DROP TABLE ${TABELA_CFOP_NEGATIVO} - > ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' >> ' || cp.ds_msg_erro ,1,4000)
 WHERE  UPPER(TRIM(cp.NM_PROCESSO))  = UPPER(TRIM('${PROCESSO}'))
  AND   cp.dt_limite_inf_nf BETWEEN to_date('${DATA_INICIO}','dd/mm/yyyy') AND to_date('${DATA_FIM}','dd/mm/yyyy')
  AND   cp.qt_registros_inf > 0
  AND   cp.qt_registros_nf  > 0;  
COMMIT;
PROMPT DROP TABLE ${TABELA_CFOP_NEGATIVO}
BEGIN
	EXECUTE IMMEDIATE 'DROP TABLE ${TABELA_CFOP_NEGATIVO}';
EXCEPTION
 WHEN OTHERS THEN
	NULL;
END;
/

ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_ini_proc      = SYSDATE,
	   cp.st_processamento = 'Create Table',
       cp.ds_msg_erro      = substr('CREATE TABLE ${TABELA_CFOP_NEGATIVO} - > ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' >> ' || cp.ds_msg_erro ,1,4000)
 WHERE  UPPER(TRIM(cp.NM_PROCESSO))  = UPPER(TRIM('${PROCESSO}'))
  AND   cp.dt_limite_inf_nf BETWEEN to_date('${DATA_INICIO}','dd/mm/yyyy') AND to_date('${DATA_FIM}','dd/mm/yyyy')
  AND   cp.qt_registros_inf > 0
  AND   cp.qt_registros_nf  > 0;
COMMIT;

PROMPT CREATE TABLE ${TABELA_CFOP_NEGATIVO} 
CREATE TABLE ${TABELA_CFOP_NEGATIVO} NOLOGGING AS
WITH tab1 AS (
      SELECT UNFE_SIG,
             INF.*,
             ROW_NUMBER() OVER(PARTITION BY inf.rowid ORDER BY cli.CADG_DAT_ATUA DESC) nu
        FROM openrisow.CLI_FORNEC_TRANSP cli,
             openrisow.ITEM_NFTL_SERV    inf
       WHERE ${FILTRO}
	     AND cli.CADG_COD = inf.cadg_cod
         AND cli.CATG_COD = inf.catg_cod
         AND cli.CADG_DAT_ATUA <= inf.infst_dtemiss
         AND inf.infst_dtemiss >=   TRUNC(to_date('${DATA_INICIO}','dd/mm/yyyy'),'MM')  
		 AND inf.infst_dtemiss <=   LAST_DAY(to_date('${DATA_FIM}','dd/mm/yyyy')) 
         AND inf.infst_ind_canc = 'N'
),
tab2 AS (
   SELECT t.*,
          trunc(dense_rank() over(partition by t.infst_serie order by t.infst_num)/1000000)+1 volume
     FROM tab1 t
    WHERE t.nu = 1
)
SELECT  tmp.emps_cod,
        tmp.fili_cod,
        tmp.infst_serie,
        tmp.estb_cod ,
        case when tmp.infst_tribicms = 'S' then tmp.infst_aliq_icms else 0 end infst_aliq_icms,
        tmp.cfop,
        tmp.unfe_sig,
        tmp.volume
       ,sum(tmp.infst_val_cont) infst_val_cont
       ,sum(tmp.infst_val_serv) infst_val_serv
       ,sum(tmp.infst_val_desc) infst_val_desc
       ,sum(tmp.infst_base_icms) infst_base_icms
       ,sum(tmp.infst_val_icms) infst_val_icms
       ,sum(tmp.infst_isenta_icms) infst_isenta_icms
       ,sum(tmp.infst_outras_icms) infst_outras_icms
       ,count(1) qtd_itens
       ,min(tmp.infst_num) infst_num_inf
       ,max(tmp.infst_num) infst_num_sup
       ,min(infst_dtemiss) infst_dtemiss_inf
       ,max(infst_dtemiss) infst_dtemiss_sup
  FROM tab2 tmp
 WHERE tmp.CFOP != '0000'
GROUP BY tmp.emps_cod,
         tmp.fili_cod,
         tmp.infst_serie,
         case when tmp.infst_tribicms = 'S' then tmp.infst_aliq_icms else 0 end,
         estb_cod ,
         tmp.cfop,
         tmp.unfe_sig,
         tmp.volume;


ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_ini_proc      = SYSDATE,
	   cp.st_processamento = 'Aguardando',
       cp.ds_msg_erro      = substr('FIM DROP/CREATE TABLE ${TABELA_CFOP_NEGATIVO} - > ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' >> ' || cp.ds_msg_erro ,1,4000)
 WHERE  UPPER(TRIM(cp.NM_PROCESSO))  = UPPER(TRIM('${PROCESSO}'))
  AND   cp.dt_limite_inf_nf BETWEEN to_date('${DATA_INICIO}','dd/mm/yyyy') AND to_date('${DATA_FIM}','dd/mm/yyyy')
  AND   cp.qt_registros_inf > 0
  AND   cp.qt_registros_nf  > 0;
COMMIT;

PROMPT Processado

exit :exit_code;

@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

