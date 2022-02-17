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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_INSERIR_COMPLVU_CLIFORNEC_DBLINK'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_comp          NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_INSERIR_COMPLVU_CLIFORNEC_DBLINK
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
	v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_INSERIR_COMPLVU_CLIFORNEC_DBLINK',1,32);
    v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
   
	l_error_count  NUMBER;    
	ex_dml_errors EXCEPTION;
	PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

	CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
	CURSOR c_sanea
    IS
	SELECT /*+ PARALLEL (8,8) */
		   *                 
    FROM   openrisow.complvu_clifornec@clone1 	
	;
	
	TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
	v_bk_sanea t_sanea;
   
    v_cp ${TABELA_CONTROLE}%ROWTYPE;
	v_ds_etapa            VARCHAR2(4000);
	PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
	BEGIN
		v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		BEGIN
			DBMS_OUTPUT.PUT_LINE(SUBSTR(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || p_ds_ddo ,1,2000));
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_APPLICATION_INFO.set_client_info(SUBSTR(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || v_ds_etapa ,1,2000));
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
   
   IF v_cp.DT_LIMITE_INF_NF = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') THEN
   
	   prc_tempo('cursor');
	   OPEN c_sanea;
	   LOOP
		  FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
		  :v_qtd_atu_comp       := :v_qtd_atu_comp + v_bk_sanea.COUNT;
		  prc_tempo('qtd_atu_comp: ' || :v_qtd_atu_comp);
		  IF v_bk_sanea.COUNT > 0 THEN
		  
			BEGIN	
				FORALL i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST SAVE EXCEPTIONS		
					INSERT INTO openrisow.complvu_clifornec
					VALUES v_bk_sanea(i);
			EXCEPTION
			   WHEN ex_dml_errors THEN
			      BEGIN 
					  l_error_count := SQL%BULK_EXCEPTIONS.count;
					  DBMS_OUTPUT.put_line('Number of failures: ' || l_error_count);
					  FOR i IN 1 .. l_error_count LOOP
						prc_tempo('Error: ' || i ||
						  ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||
						  ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE));
					  END LOOP;
				  EXCEPTION
				  WHEN OTHERS THEN
				     NULL;
				  END;
			END;
			${COMMIT};	
		  END IF;

		  EXIT WHEN c_sanea%NOTFOUND;	  

	   END LOOP;

	   CLOSE c_sanea;

   END IF;
   
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_atu_comp);

   DBMS_APPLICATION_INFO.set_module(null,null);
   DBMS_APPLICATION_INFO.set_client_info (null);

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
      :v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || :v_msg_erro,1,4000);
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
	   cp.qt_atualizados_comp    = NVL(cp.qt_atualizados_comp,0)     + :v_qtd_atu_comp
 WHERE cp.rowid = '${ROWID_CP}'
   AND cp.st_processamento = 'Em Processamento'
   AND cp.NM_PROCESSO = '${PROCESSO}';
COMMIT;


PROMPT Processado

exit :exit_code;

@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

