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
var v_st_processamento    VARCHAR2(50)   = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_CORRIGI_FILI_COD'
var exit_code             NUMBER         = 0
var v_qtd_processados     NUMBER         = 0
var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_ins_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_reg_paralizacao NUMBER         = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_CORRIGI_FILI_COD
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE   

	v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_CORRIGI_FILI_COD',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
	
    CONSTANTE_LIMIT PLS_INTEGER := 25000; 
	
    CURSOR c_nf
       IS
		SELECT /*+ parallel(15) */ nf.rowid rowid_nf , inf.rowid rowid_inf 
			FROM openrisow.item_nftl_serv  PARTITION (${PARTICAO_INF}) inf ,
			     openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf
			WHERE  ${FILTRO}
			 AND nf.fili_cod                                    = '9144'
			 AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x'))      = 'IN1'
			 AND ( 
		          ( nf.mnfst_dtemiss >= to_date('01/04/2017','dd/mm/yyyy') AND nf.mnfst_dtemiss <= to_date('31/12/2017','dd/mm/yyyy') ) 
		      OR  ( nf.mnfst_dtemiss >= to_date('01/01/2018','dd/mm/yyyy') AND nf.mnfst_dtemiss <= to_date('31/12/2019','dd/mm/yyyy') ) 
		     ) 
			 AND inf.emps_cod        = nf.emps_cod
		     AND inf.fili_cod        = nf.fili_cod
		     AND inf.infst_serie     = nf.mnfst_serie
		     AND inf.infst_num       = nf.mnfst_num
		     AND inf.infst_dtemiss   = nf.mnfst_dtemiss;
			
	
	
    v_nf                 c_nf%ROWTYPE;
	TYPE t_nf IS TABLE OF c_nf%ROWTYPE INDEX BY PLS_INTEGER;
	v_bk_nf              t_nf;
    v_altera_nf          BOOLEAN       := FALSE;
	v_exists             NUMBER        := 0;  
    v_ds_etapa           VARCHAR2(4000);
    PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
    BEGIN
      v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
      DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
    EXCEPTION
      WHEN OTHERS THEN
	    NULL;
    END;
   

BEGIN
   -----------------------------------------------------------------------------
   --> Nomeando o processo
   -----------------------------------------------------------------------------	
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);	
   prc_tempo('INICIO'); 
   OPEN c_nf;
   LOOP
	  FETCH c_nf BULK COLLECT INTO v_bk_nf LIMIT CONSTANTE_LIMIT;   
	  :v_qtd_processados       := :v_qtd_processados + v_bk_nf.COUNT;
	  IF v_bk_nf.COUNT > 0 THEN
		:v_qtd_atu_nf := :v_qtd_atu_nf + v_bk_nf.COUNT;
	     FORALL i IN v_bk_nf.FIRST .. v_bk_nf.LAST			
			UPDATE openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf 
				SET nf.var05 = substr('rIN1_0001: 9144 ' || '>>'|| nf.VAR05,1,150) 
				  , nf.fili_cod = '0001'	 
				WHERE rowid = v_bk_nf(i).rowid_nf;
		

		FORALL i IN v_bk_nf.FIRST .. v_bk_nf.LAST			
			UPDATE openrisow.item_nftl_serv  PARTITION (${PARTICAO_INF}) inf 
				SET inf.var05 = substr('rIN1_0001: 9144 ' || '>>'|| inf.VAR05,1,150) 
				  , inf.fili_cod = '0001'	 
				WHERE rowid = v_bk_nf(i).rowid_inf;
				
		  ${COMMIT};
	  END IF;
	  EXIT WHEN c_nf%NOTFOUND;	  
   END LOOP; 	  
   CLOSE c_nf;   
   ${COMMIT};	
   prc_tempo('FIM');
   prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> NF : ' || :v_qtd_atu_nf );
   -----------------------------------------------------------------------------
   --> Eliminando a nomeação
   -----------------------------------------------------------------------------
   DBMS_APPLICATION_INFO.set_module(null,null);
   DBMS_APPLICATION_INFO.set_client_info (null);   
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500) || ' - rowid_inf >> ' || v_nf.rowid_nf);
      :v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || :v_msg_erro,1,4000);
      :v_st_processamento := 'Erro';
      :exit_code := 1;
	  -----------------------------------------------------------------------------
	  --> Eliminando a nomeação
	  -----------------------------------------------------------------------------
	  DBMS_APPLICATION_INFO.set_module(null,null);
	  DBMS_APPLICATION_INFO.set_client_info (null);	  
END;
/

PROMPT Processado
ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc          = SYSDATE,
       cp.st_processamento     = :v_st_processamento,
       cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,1000) || ' >> ' || cp.ds_msg_erro ,1,4000),
       cp.qt_atualizados_nf    = NVL(cp.qt_atualizados_nf,0)   + :v_qtd_atu_nf,
       cp.qt_atualizados_inf   = NVL(cp.qt_atualizados_inf,0)  + :v_qtd_atu_inf,
       cp.qt_atualizados_cli   = NVL(cp.qt_atualizados_cli,0)  + :v_qtd_atu_cli,
       cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp--,
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

