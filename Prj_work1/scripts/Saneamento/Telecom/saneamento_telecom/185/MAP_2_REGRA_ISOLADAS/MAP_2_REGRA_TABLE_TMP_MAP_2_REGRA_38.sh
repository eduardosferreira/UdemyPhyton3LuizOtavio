#!/bin/bash
echo ${DBLINK1}
echo ${TABELA_CONTROLE} ${TABELA_CFOP_NEGATIVO}
sqlplus -S /nolog <<@EOF >> ${SPOOL_FILE}.log 2>> ${SPOOL_FILE}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_TABLE_TMP_MAP_2_REGRA_38.sh'
var exit_code             NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_TABLE_TMP_MAP_2_REGRA_38.sh  ${TABELA_CONTROLE} ${TABELA_CFOP_NEGATIVO}
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
	WITH 
	    TMP AS (  SELECT 
		            c.emps_cod            AS emps_cod, 
					'0001'                AS fili_cod,
					22                    AS mdoc_cod,
					TRUNC(c.mes_ano,'MM') AS mnfst_dtemiss,
					c.id_arq_conv115 
	             FROM gfcarga.tsh_controle_arq_conv_11516${DBLINK1} c
				 WHERE c.area        = 'PROTOCOLADO' 
				 AND   c.emps_cod    = 'TBRA'
				 AND TRUNC(c.mes_ano,'MM') = TRUNC(to_date('${DATA_INICIO}','dd/mm/yyyy'),'MM')
			    ) ,
	    TMP_MAP AS ( SELECT 		TMP.emps_cod, 
									TMP.fili_cod,
									TMP.mdoc_cod,
									MAX(TMP.mnfst_dtemiss)   AS mnfst_dtemiss, 
									TO_NUMBER(mes.numero_nf) AS mnfst_num,
									UPPER(TRIM(TRANSLATE(mes.serie,'x ','x')))  AS mnfst_serie,                            
									SUM(NVL(mes.base_icms, 0)) mnfst_val_basicms,
									SUM(NVL(mes.valor_icms, 0)) mnfst_val_icms,
									SUM(NVL(mes.isentas_icms, 0)) mnfst_val_isentas ,
									SUM(NVL(mes.valor_total, 0) - NVL(mes.desconto, 0)) mnfst_val_tot                            
								FROM  gfcarga.tsh_item_conv_11516${DBLINK1} mes, TMP
								WHERE mes.sit_doc           = 'N' 
								AND   mes.id_arq_conv115    = TMP.id_arq_conv115                       
							  GROUP BY TMP.emps_cod, 
									   TMP.fili_cod,
									   TMP.mdoc_cod,
									   mes.serie,
									   mes.numero_nf
              )	
SELECT /*+ parallel(15) */ nf.* FROM TMP_MAP nf WHERE ${FILTRO};

-- Create/Recreate indexes 
create index ${TABELA_CFOP_NEGATIVO}_I1 on ${TABELA_CFOP_NEGATIVO} (mnfst_dtemiss);
create index ${TABELA_CFOP_NEGATIVO}_I2 on ${TABELA_CFOP_NEGATIVO} (TRUNC(mnfst_dtemiss,'MM'));

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

