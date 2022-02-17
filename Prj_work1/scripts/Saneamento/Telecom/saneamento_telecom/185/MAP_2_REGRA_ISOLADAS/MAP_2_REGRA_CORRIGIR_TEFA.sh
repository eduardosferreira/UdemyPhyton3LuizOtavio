#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
FILTRO_SCRIPT="${4:-${FILTRO}}"


####turn it on
###shopt -s extglob
###CRITERIOS_SCRIPT="${FILTRO_SCRIPT}"
###CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT^^}"
###### Trim leading whitespaces ###
###CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT##*( )}" 
###### trim trailing whitespaces  ##
###CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT%%*( )}"
###CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT^^}" 	
###CRITERIOS_SCRIPT="AND ${CRITERIOS_SCRIPT} AND " 	
###
###REGRAS_SCRIPT="1=1"
###CRITERIOS_AUX=""
###FINDUNICO="AND"
###REPLACEUNICO="|"
######CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT//$FINDUNICO/$REPLACEUNICO}"
###
###echo "${CRITERIOS_SCRIPT}"
###
###IFS='|' # colon (|) is set as delimiter
###read -ra ADDR <<< "$CRITERIOS_SCRIPT" # str is read into an array as tokens separated by IFS
###
### for i in "${ADDR[@]}"; do # access each element of array
### 
###     CRITERIOS_AUX="$i"
### 	### Trim leading whitespaces ###
### 	CRITERIOS_AUX="${CRITERIOS_AUX##*( )}" 
### 	### trim trailing whitespaces  ##
### 	CRITERIOS_AUX="${CRITERIOS_AUX%%*( )}"
### 	CRITERIOS_AUX="${CRITERIOS_AUX^^}" 	
### 	case ${CRITERIOS_AUX} in
###     	   *"FILI_COD"*|*"EMPS_COD"*)
### 	    REGRAS_SCRIPT="${REGRAS_SCRIPT^^} AND ${CRITERIOS_AUX}" 
### 	;;
###     	*)
###         REGRAS_SCRIPT="${REGRAS_SCRIPT^^}" 
### 	;;
### 	esac
### 
### 	echo "${CRITERIOS_AUX}"
### done
### echo "${REGRAS_SCRIPT}"
### 
### IFS=' ' # reset to default value after usage
### 
### # turn it off
### shopt -u extglob
### 
### FINDUNICO="|"
### REPLACEUNICO=","


sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_CORRIGIR_TEFA'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_CORRIGIR_TEFA
PROMPT ### Inicio do processo ${0}  ###
PROMPT
BEGIN 
 UPDATE ${TABELA_CONTROLE} cp
   SET cp.ds_msg_erro            = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0) + :v_qtd_processados
 WHERE cp.rowid = '${ROWID_CP}';
 COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE ${TABELA_CONTROLE}  
		SET ds_msg_erro              = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(ds_msg_erro,1,990) ,1,4000),
			qt_cad_nao_encontrado    = NVL(qt_cad_nao_encontrado,0) + :v_qtd_processados
		WHERE dt_limite_inf_nf       = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') 
		AND UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'))
        AND qt_registros_inf > 0
        AND qt_registros_nf  > 0		
		AND ROWNUM < 2;
		COMMIT;
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;          
	END;
END;
/

DECLARE

   v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_CORRIGIR_TEFA',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
    
   l_error_count  NUMBER;    
   ex_dml_errors  EXCEPTION;
   PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
   v_error_bk     VARCHAR2(4000);
	
   CONSTANTE_LIMIT PLS_INTEGER := 50000; 
  
   v_data_aux DATE := null;
   
   CURSOR c_sanea IS		
			SELECT A.*
					 FROM (SELECT ROW_NUMBER() OVER(PARTITION BY A.emps_cod, A.fili_cod, A.mnfst_serie, A.mnfst_dtemiss, A.mnfst_num ORDER BY TO_NUMBER(A.TEFA_COD_AREA_T), TO_NUMBER(A.TEFA_TERMINAL_T)) rnk,
								  A.TEFA_COD_AREA_T AS COD_AREA_FILHO,
								  A.TEFA_TERMINAL_T AS TERMINAL_FILHO,
								  A.TEFA_COD_AREA AS COD_AREA_PAI,
								  A.TEFA_TERMINAL AS TEFA_TERMINAL_PAI,
								  A.*
							 FROM (SELECT 'TEFA' cli, nf.*
									 FROM openrisow.tefa nf
									WHERE ${FILTRO_SCRIPT}
									 and nf.mnfst_dtemiss BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') and  TO_DATE('${DATA_FIM}','DD/MM/YYYY')
									  and exists (select 1
												 from openrisow.tefa nf2
												 where nf2.emps_cod =nf.emps_cod
												   and nf2.fili_cod = nf.fili_cod
												   and nf2.mnfst_serie = nf.mnfst_serie
												   and nf2.mnfst_dtemiss = nf.mnfst_dtemiss
												   and nf2.mnfst_num = nf.mnfst_num
												   and ( length(translate(nf2.TEFA_COD_AREA, '0123456789 ', ' '))>0  OR length(translate(nf2.TEFA_TERMINAL, '0123456789', ' '))>0)
												  )
									  ) A
							ORDER BY A.emps_cod, A.fili_cod, A.mnfst_serie, A.mnfst_dtemiss, A.mnfst_num) A
					WHERE 1=1
					 and A.rnk = 1
					ORDER BY A.emps_cod, A.fili_cod, A.mnfst_serie, A.mnfst_dtemiss, A.mnfst_num, rnk;
		
   TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_sanea t_sanea;
   v_sanea    c_sanea%ROWTYPE;
	
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

   -- Inicializacao
   prc_tempo('Inicializacao');

   -----------------------------------------------------------------------------
   --> Nomeando o processo
   -----------------------------------------------------------------------------	
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);
   
   IF NVL(LENGTH(TRIM('${ROWID_CP}')),0) > 0 THEN
	   BEGIN
		 SELECT cp.DT_LIMITE_INF_NF
		 INTO   v_data_aux
		 FROM   ${TABELA_CONTROLE} cp
		 WHERE  cp.rowid = '${ROWID_CP}';   
	   EXCEPTION
			WHEN OTHERS THEN
				v_data_aux := TO_DATE('${DATA_INICIO}','DD/MM/YYYY');
	   END;
   ELSE
	   v_data_aux := TO_DATE('${DATA_INICIO}','DD/MM/YYYY');	
   END IF;
   
   -- CP
   prc_tempo('SANEA');
   IF v_data_aux = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') THEN
	   OPEN c_sanea;
	   LOOP
		  FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
		  :v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
		  IF v_bk_sanea.COUNT > 0 THEN
		  
			FORALL i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST 
				
				UPDATE openrisow.TEFA tef SET tef.TEFA_COD_AREA = v_bk_sanea(i).TEFA_COD_AREA_T, tef.TEFA_TERMINAL = v_bk_sanea(i).TEFA_TERMINAL_T
				WHERE tef.emps_cod =v_bk_sanea(i).emps_cod
				  and tef.fili_cod = v_bk_sanea(i).fili_cod
				  and tef.mnfst_serie = v_bk_sanea(i).mnfst_serie
				  and tef.mnfst_dtemiss = v_bk_sanea(i).mnfst_dtemiss
				  and tef.mnfst_num = v_bk_sanea(i).mnfst_num;
				
		  END IF;       
		  ${COMMIT};	
		  EXIT WHEN c_sanea%NOTFOUND;	  
	   END LOOP;        
	   CLOSE c_sanea;   
   END IF;
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_processados);
   :v_msg_erro :=   substr(substr(nvl(v_ds_etapa,' '),1,3000) || ' <||> ' || substr(:v_msg_erro,1,990) ,1,4000);
   -----------------------------------------------------------------------------
   --> Eliminando a nomeação
   -----------------------------------------------------------------------------
   DBMS_APPLICATION_INFO.set_module(null,null);
   DBMS_APPLICATION_INFO.set_client_info (null);   
EXCEPTION           
   WHEN OTHERS THEN 
      ROLLBACK;     
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
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
BEGIN 
 UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc            = SYSDATE,
       cp.st_processamento       = :v_st_processamento,
       cp.ds_msg_erro            = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0) + :v_qtd_processados
 WHERE cp.rowid = '${ROWID_CP}';
 COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE ${TABELA_CONTROLE}  
		SET ds_msg_erro              = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(ds_msg_erro,1,990) ,1,4000),
			qt_cad_nao_encontrado    = NVL(qt_cad_nao_encontrado,0) + :v_qtd_processados
		WHERE dt_limite_inf_nf       = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') 
		AND UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'))
        AND qt_registros_inf > 0
        AND qt_registros_nf  > 0		
		AND ROWNUM < 2;
		COMMIT;
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;          
	END;
END;
/
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF                

RETORNO=$?

${WAIT}

exit ${RETORNO}

