#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}

SCRIPT_PARTICAO_NF="${PARTICAO_NF}"
SCRIPT_PARTICAO_INF="${PARTICAO_INF}"

if [ "${SCRIPT_PARTICAO_NF}" != "" ]; then
	SCRIPT_PARTICAO_NF=" PARTITION (${SCRIPT_PARTICAO_NF}) "
else
	SCRIPT_PARTICAO_NF=" "
fi

if [ "${SCRIPT_PARTICAO_INF}" != "" ]; then
	SCRIPT_PARTICAO_INF=" PARTITION (${SCRIPT_PARTICAO_INF}) "
else
	SCRIPT_PARTICAO_INF="  "
fi

sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50)   = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_31_AJUSTE_TP_UTILIZACAO_MESTRE'
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
PROMPT MAP_2_REGRA_31_AJUSTE_TP_UTILIZACAO_MESTRE
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT
BEGIN 
 UPDATE ${TABELA_CONTROLE} cp
   SET cp.ds_msg_erro            = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_atualizados_nf      = NVL(cp.qt_atualizados_nf,0) + :v_qtd_atu_nf
 WHERE cp.rowid = '${ROWID_CP}';
 COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE ${TABELA_CONTROLE}  
		SET ds_msg_erro              = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(ds_msg_erro,1,990) ,1,4000),
			qt_atualizados_nf        = NVL(qt_atualizados_nf,0) + :v_qtd_atu_nf,
            st_processamento         = :v_st_processamento
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

	v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_31',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
	
    CONSTANTE_LIMIT PLS_INTEGER := 1000; 
	
    CURSOR c_nf
       IS
		WITH tab1 AS
		  (SELECT *
		  FROM
			(SELECT 
			  /*+ index(nf MESTRE_NFTL_SERVP1) CURSOR_SHARING_FORCE */ 
			  nf.rowid rowid_nf,
			  nf.emps_cod,
			  nf.fili_cod,
			  nf.mnfst_serie,
			  nf.mnfst_num,
			  nf.mnfst_dtemiss,
			  nf.mdoc_cod,
			  nf.mnfst_tip_util,
			  nf.cadg_cod,
			  nf.catg_cod,
			  comp.cadg_dat_atua,
			  comp.cadg_tip_utiliz,
			  ROW_NUMBER() OVER (PARTITION BY nf.rowid ORDER BY comp.cadg_dat_atua DESC) nu
			FROM openrisow.complvu_clifornec comp,
			     openrisow.mestre_nftl_serv ${SCRIPT_PARTICAO_NF} nf
			WHERE  ${FILTRO}
			AND nf.mnfst_dtemiss >= TO_DATE('${DATA_INICIO}','DD/MM/YYYY') 
			AND nf.mnfst_dtemiss <= TO_DATE('${DATA_FIM}','DD/MM/YYYY') 
			AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1')
			AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
			AND comp.cadg_cod                                  = nf.cadg_cod
			AND comp.catg_cod                                  = nf.catg_cod
			AND comp.cadg_dat_atua                            <= nf.mnfst_dtemiss
			) tmp
		  WHERE nu = 1
		  )
	SELECT 
    /*+ parallel(15) */ 
	*
    FROM (SELECT		  
		  tmp.rowid_nf,
		  TRIM(TO_CHAR(MIN(tmp.mnfst_tip_util))) mnfst_tip_util_old,
		  (CASE
		    WHEN MAX(tmp.serv_cod_exists_inf_nf) = 'N'  THEN 
				CASE
					WHEN MAX(tmp.serv_cod_exists_inf_cli) = 'S'  THEN 
					     TRIM(TO_CHAR(MIN(tmp.cadg_tip_utiliz)))
					ELSE TRIM(TO_CHAR(MIN(tmp.servtl_tip_utiliz)))
				END 
			ELSE TRIM(TO_CHAR(MIN(tmp.mnfst_tip_util)))
		  END) mnfst_tip_util_new
		FROM
		  (SELECT tab1.rowid_nf,
			inf.rowid AS rowid_inf,
			st.servtl_tip_utiliz,
			tab1.mnfst_tip_util,
			tab1.cadg_tip_utiliz,
			(CASE
                WHEN TRIM(TO_CHAR(tab1.mnfst_tip_util)) IS NOT NULL THEN			
					CASE
						WHEN NVL(TRIM(TO_CHAR(st.servtl_tip_utiliz)),'1') = TRIM(TO_CHAR(tab1.mnfst_tip_util)) THEN 
						  'S' 
						ELSE 
						  'N'
					END
			 ELSE 
			    'N' 
			 END) serv_cod_exists_inf_nf,			
			(CASE
                WHEN TRIM(TO_CHAR(tab1.cadg_tip_utiliz)) IS NOT NULL THEN			
					CASE
						WHEN NVL(TRIM(TO_CHAR(st.servtl_tip_utiliz)),'1') = NVL(TRIM(TO_CHAR(tab1.cadg_tip_utiliz)),'1') THEN 
						  'S' 
						ELSE 
						  'N'
					END
			 ELSE 
			    'N' 
			 END) serv_cod_exists_inf_cli,
			ROW_NUMBER() OVER (PARTITION BY inf.rowid ORDER BY st.servtl_dat_atua DESC) nu1
		  FROM openrisow.servico_telcom st,
			   openrisow.item_nftl_serv  ${SCRIPT_PARTICAO_INF} inf ,
			   tab1
		  WHERE inf.emps_cod      = tab1.emps_cod
		  AND inf.fili_cod        = tab1.fili_cod
		  AND inf.infst_serie     = tab1.mnfst_serie
		  AND inf.infst_num       = tab1.mnfst_num
		  AND inf.infst_dtemiss   = tab1.mnfst_dtemiss
		  AND st.emps_cod         = inf.emps_cod
		  AND st.fili_cod         = inf.fili_cod
		  AND st.servtl_cod       = inf.serv_cod
		  AND st.servtl_dat_atua <= inf.infst_dtemiss
		  ) tmp
		WHERE tmp.nu1 = 1 -- AND tmp.servtl_tip_utiliz IS NOT NULL
		GROUP BY tmp.rowid_nf
		 HAVING  MAX(tmp.serv_cod_exists_inf_nf) = 'N')
	WHERE MNFST_TIP_UTIL_OLD != MNFST_TIP_UTIL_NEW;
	
	
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
			UPDATE openrisow.mestre_nftl_serv nf 
				SET nf.var05 = substr('r2015_31u:' || v_bk_nf(i).mnfst_tip_util_old  || '>>'|| nf.VAR05,1,150) 
				  , nf.mnfst_tip_util = v_bk_nf(i).mnfst_tip_util_new	 
				WHERE rowid = v_bk_nf(i).rowid_nf;
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
BEGIN 
 UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc            = SYSDATE,
       cp.st_processamento       = :v_st_processamento,
	   cp.ds_msg_erro            = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_atualizados_nf      = NVL(cp.qt_atualizados_nf,0) + :v_qtd_atu_nf
 WHERE cp.rowid = '${ROWID_CP}';
 COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE ${TABELA_CONTROLE}  
		SET dt_fim_proc              = SYSDATE,
			st_processamento         = DECODE(TRIM(UPPER(:v_st_processamento)),'ERRO',:v_st_processamento,'Processado'),
			ds_msg_erro              = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(ds_msg_erro,1,990) ,1,4000),
			qt_atualizados_nf        = NVL(qt_atualizados_nf,0) + :v_qtd_atu_nf
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

