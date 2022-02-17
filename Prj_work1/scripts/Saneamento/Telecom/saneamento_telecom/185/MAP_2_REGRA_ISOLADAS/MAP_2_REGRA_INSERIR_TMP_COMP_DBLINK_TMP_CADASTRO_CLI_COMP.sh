#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_INSERIR_TMP_COMP_DBLINK_TMP_CADASTRO_CLI_COMP.sh'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_atu_cli         NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_INSERIR_TMP_COMP_DBLINK_TMP_CADASTRO_CLI_COMP.sh
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
   v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_INSERIR_TMP_COMP_DBLINK_TMP_CADASTRO_CLI_COMP.sh',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
   
   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   CURSOR c_sanea
       IS
	WITH TMP_TAB1 AS (	SELECT DISTINCT A.ROWID_COMP
	FROM GFCADASTRO.TMP_CADASTRO_CLI_COMP@C7 A
	WHERE TRUNC(A.DT_PERIODO)  BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY')
	UNION 	
	SELECT  DISTINCT A.ROWID_COMP
	FROM GFCADASTRO.TMP_CADASTRO_CLI_COMP@C7 A
	WHERE ( TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM') = TO_DATE('01/01/2015','DD/MM/YYYY') 
		    AND TRUNC(A.DT_PERIODO) < TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM'))
	)
	SELECT /*+ PARALLEL (15) */ A.* FROM TMP_TAB1 T, OPENRISOW.COMPLVU_CLIFORNEC@C7 A 
	WHERE A.ROWID = T.ROWID_COMP
	;
   TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_sanea t_sanea;
   
   v_cp                  ${TABELA_CONTROLE}%ROWTYPE;
   v_ds_stage            VARCHAR2(4000);
   PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2)
	AS
	BEGIN
		BEGIN
			v_ds_stage := SUBSTR(p_ds_ddo || ' >> ' || v_ds_stage,1,4000);
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_OUTPUT.PUT_LINE(SUBSTR(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || p_ds_ddo ,1,2000));
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_APPLICATION_INFO.set_client_info(SUBSTR(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || v_ds_stage ,1,2000));
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
	EXCEPTION
		WHEN OTHERS THEN
			NULL;
	END;   
	
BEGIN

   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);

   prc_tempo('inicio');  
   
   SELECT *
   INTO   v_cp
   FROM   ${TABELA_CONTROLE} cp
   WHERE  cp.rowid = '${ROWID_CP}';
   
   IF v_cp.DT_LIMITE_INF_NF  = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') THEN
	   prc_tempo('cursor');
	   OPEN c_sanea;
	   LOOP
			FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
			:v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
			prc_tempo('CLI >> ' || :v_qtd_processados || ' >> ' || :v_qtd_atu_cli || ' >> ' || :v_qtd_atu_comp);
			IF v_bk_sanea.COUNT > 0 THEN
				FORALL i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST
					INSERT   INTO  OPENRISOW.TMP_K_C7_COMPLVU_CLIFORNEC 
					VALUES v_bk_sanea(i);  
				:v_qtd_atu_cli       := :v_qtd_atu_cli + v_bk_sanea.COUNT;
			END IF;
			${COMMIT};	
			EXIT WHEN c_sanea%NOTFOUND;	  
	   END LOOP;
	   CLOSE c_sanea;
   END IF;
   
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_processados);

   DBMS_APPLICATION_INFO.set_module(null,null);
   DBMS_APPLICATION_INFO.set_client_info (null);

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
      :v_msg_erro := SUBSTR(v_ds_stage || ' >> ' || :v_msg_erro,1,4000);
      :v_st_processamento := 'Erro';
      :exit_code := 1;

      DBMS_APPLICATION_INFO.set_module(null,null);
      DBMS_APPLICATION_INFO.set_client_info (null);

END;
/

PROMPT Processado
ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc = SYSDATE,
       cp.st_processamento = :v_st_processamento,
       cp.ds_msg_erro = substr(substr(nvl(:v_msg_erro,' '),1,1000) || cp.ds_msg_erro ,1,4000),
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_nf,0) + :v_qtd_processados,
	   cp.qt_atualizados_cli = NVL(cp.qt_atualizados_cli,0) + :v_qtd_atu_cli,
	   cp.qt_atualizados_comp = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp
 WHERE cp.rowid = '${ROWID_CP}'
   AND cp.NM_PROCESSO = '${PROCESSO}';
COMMIT;


PROMPT Processado

exit :exit_code;

@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

