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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_INSERIR_ST_DBLINK'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_st          NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_INSERIR_ST_DBLINK
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
   v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_INSERIR_ST_DBLINK',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
   
   l_error_count  NUMBER;    
   ex_dml_errors EXCEPTION;
   PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   v_servtl_dat_atua  openrisow.servico_telcom.servtl_dat_atua%type;
   
   CURSOR c_st(p_servtl_dat_atua in openrisow.servico_telcom.servtl_dat_atua%type)
       IS
	SELECT  /*+ PARALLEL (nf,8,8) */
	 st.*  
    FROM  openrisow.servico_telcom${DBLINK1} st  
	WHERE ${FILTRO} 
	AND NOT EXISTS (SELECT 1 FROM openrisow.servico_telcom st1 WHERE st1.fili_cod = st.fili_cod	  AND  st1.servtl_cod = st.servtl_cod AND  st1.servtl_dat_atua  = st.servtl_dat_atua) ;
   TYPE t_st IS TABLE OF c_st%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_st t_st;
   
   v_ds_etapa            VARCHAR2(4000);
   PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
   BEGIN
     v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
     DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
   EXCEPTION
     WHEN OTHERS THEN
	   NULL;
   END;
   
BEGIN

   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);

   prc_tempo('inicio');  
   
   SELECT cp.DT_LIMITE_INF_NF
   INTO   v_servtl_dat_atua
   FROM   ${TABELA_CONTROLE} cp
   WHERE  cp.rowid = '${ROWID_CP}';
   
   IF v_servtl_dat_atua = TRUNC(v_servtl_dat_atua,'MM') THEN
	   prc_tempo('cursor');
	   OPEN c_st(p_servtl_dat_atua => v_servtl_dat_atua);
	   LOOP
		  FETCH c_st BULK COLLECT INTO v_bk_st LIMIT CONSTANTE_LIMIT;   
		  :v_qtd_atu_st       := :v_qtd_atu_st + v_bk_st.COUNT;
		  IF v_bk_st.COUNT > 0 THEN
			BEGIN	
				FORALL i IN v_bk_st.FIRST .. v_bk_st.LAST SAVE EXCEPTIONS		
					INSERT INTO openrisow.servico_telcom (emps_cod, 
                                                          fili_cod, 
                                                          servtl_dat_atua, 
					                                      servtl_cod, 
					                                      clasfi_cod, 
														  servtl_desc,
                                                          servtl_compl, 
														  servtl_ind_tprec, 
                                                          servtl_ind_tpserv, 
                                                          servtl_cod_nat, 
                                                          var01, 
                                                          var02,
														  var03, 
                                                          var04, 
														  var05, 
														  num01, 
                                                          num02, 
                                                          num03, 
														  servtl_ind_rec,
														  servtl_tip_utiliz)									
					VALUES (v_bk_st(i).emps_cod, 
						v_bk_st(i).fili_cod, 
						v_bk_st(i).servtl_dat_atua, 
					    v_bk_st(i).servtl_cod, 
					    v_bk_st(i).clasfi_cod, 
						v_bk_st(i).servtl_desc,
                        v_bk_st(i).servtl_compl, 
						v_bk_st(i).servtl_ind_tprec, 
                        v_bk_st(i).servtl_ind_tpserv, 
                        v_bk_st(i).servtl_cod_nat, 
                        v_bk_st(i).var01, 
                        v_bk_st(i).var02,
						v_bk_st(i).var03, 
                        v_bk_st(i).var04, 
						v_bk_st(i).var05, 
						v_bk_st(i).num01, 
                        v_bk_st(i).num02, 
                        v_bk_st(i).num03, 
						v_bk_st(i).servtl_ind_rec,
					    v_bk_st(i).servtl_tip_utiliz);				
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
		  END IF;
		  ${COMMIT};	
		  EXIT WHEN c_st%NOTFOUND;	  
	   END LOOP;
	   CLOSE c_st;
   END IF;
   
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_atu_st);

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
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_st,0) + :v_qtd_atu_st
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

