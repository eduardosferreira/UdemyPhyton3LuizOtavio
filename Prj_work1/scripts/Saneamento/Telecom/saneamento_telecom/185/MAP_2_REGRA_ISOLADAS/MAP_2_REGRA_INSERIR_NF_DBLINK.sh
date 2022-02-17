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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_INSERIR_NF_DBLINK'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_INSERIR_NF_DBLINK
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
   v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_INSERIR_NF_DBLINK',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
   
   l_error_count  NUMBER;    
   ex_dml_errors EXCEPTION;
   PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   v_mnfst_dtemiss  openrisow.mestre_nftl_serv.mnfst_dtemiss%type;
   
   CURSOR c_nf(p_mnfst_dtemiss in openrisow.mestre_nftl_serv.mnfst_dtemiss%type)
       IS
	SELECT  /*+ PARALLEL (nf,8,8) */
	 nf.*  
    FROM  openrisow.mestre_nftl_serv${DBLINK1} nf
	WHERE  ${FILTRO} AND trunc(nf.mnfst_dtemiss,'MM') = trunc(p_mnfst_dtemiss,'MM');
   TYPE t_nf IS TABLE OF c_nf%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_nf t_nf;
   
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
   INTO   v_mnfst_dtemiss
   FROM   ${TABELA_CONTROLE} cp
   WHERE  cp.rowid = '${ROWID_CP}';
   
   IF v_mnfst_dtemiss = TRUNC(v_mnfst_dtemiss,'MM') THEN
	   prc_tempo('cursor');
	   OPEN c_nf(p_mnfst_dtemiss => v_mnfst_dtemiss);
	   LOOP
		  FETCH c_nf BULK COLLECT INTO v_bk_nf LIMIT CONSTANTE_LIMIT;   
		  :v_qtd_atu_nf       := :v_qtd_atu_nf + v_bk_nf.COUNT;
		  IF v_bk_nf.COUNT > 0 THEN
			BEGIN	
				FORALL i IN v_bk_nf.FIRST .. v_bk_nf.LAST SAVE EXCEPTIONS		
					INSERT INTO OPENRISOW.mestre_nftl_serv (EMPS_COD             , 
						 FILI_COD              , 
						 TDOC_COD              , 
						 MNFST_SERIE           , 
						 MNFST_NUM             , 
						 MNFST_DTEMISS         , 
						 CATG_COD              , 
						 CADG_COD              , 
						 MNFST_IND_CONT        , 
						 MDOC_COD              , 
						 MNFST_VAL_TOT         , 
						 MNFST_VAL_DESC        , 
						 MNFST_IND_CANC        , 
						 MNFST_DAT_VENC        , 
						 MNFST_PER_REF         , 
						 MNFST_AVISTA          , 
						 NUM01                 , 
						 NUM02                 , 
						 NUM03                 , 
						 VAR01                 , 
						 VAR02                 , 
						 VAR03                 , 
						 VAR04                 , 
						 VAR05                 , 
						 MNFST_IND_CNV115      , 
						 CNPJ_CPF              , 
						 MNFST_VAL_BASICMS     , 
						 MNFST_VAL_ICMS        , 
						 MNFST_VAL_ISENTAS     , 
						 MNFST_VAL_OUTRAS      , 
						 MNFST_CODH_NF         , 
						 MNFST_CODH_REGNF      , 
						 MNFST_CODH_REGCLI     , 
						 MNFST_REG_ESP         , 
						 MNFST_BAS_ICMS_ST     , 
						 MNFST_VAL_ICMS_ST     , 
						 MNFST_VAL_PIS         , 
						 MNFST_VAL_COFINS      , 
						 MNFST_VAL_DA          , 
						 MNFST_VAL_SER         , 
						 MNFST_VAL_TERC        , 
						 CICD_COD_INF          , 
						 MNFST_TIP_ASSI        , 
						 MNFST_TIP_UTIL        , 
						 MNFST_GRP_TENS        , 
						 MNFST_IND_EXTEMP      , 
						 MNFST_DAT_EXTEMP      , 
						 MNFST_NUM_FIC         , 
						 MNFST_DT_LT_ANT       , 
						 MNFST_DT_LT_ATU       , 
						 MNFST_NUM_FAT         , 
						 MNFST_VL_TOT_FAT      , 
						 MNFST_CHV_NFE         , 
						 MNFST_DAT_AUT_NFE     , 
						 MNFST_VAL_DESC_PIS    , 
						 MNFST_VAL_DESC_COFINS ,
						 MNFST_FIN_DOCE        ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
						 MNFST_CHV_REF         ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
						 MNFST_IND_DEST           -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
						 ) 
					VALUES ( v_bk_nf(i).EMPS_COD             , 
						 v_bk_nf(i).FILI_COD              , 
						 v_bk_nf(i).TDOC_COD              , 
						 v_bk_nf(i).MNFST_SERIE           , 
						 v_bk_nf(i).MNFST_NUM             , 
						 v_bk_nf(i).MNFST_DTEMISS         , 
						 v_bk_nf(i).CATG_COD              , 
						 v_bk_nf(i).CADG_COD              , 
						 v_bk_nf(i).MNFST_IND_CONT        , 
						 v_bk_nf(i).MDOC_COD              , 
						 v_bk_nf(i).MNFST_VAL_TOT         , 
						 v_bk_nf(i).MNFST_VAL_DESC        , 
						 v_bk_nf(i).MNFST_IND_CANC        , 
						 v_bk_nf(i).MNFST_DAT_VENC        , 
						 v_bk_nf(i).MNFST_PER_REF         , 
						 v_bk_nf(i).MNFST_AVISTA          , 
						 v_bk_nf(i).NUM01                 , 
						 v_bk_nf(i).NUM02                 , 
						 v_bk_nf(i).NUM03                 , 
						 v_bk_nf(i).VAR01                 , 
						 v_bk_nf(i).VAR02                 , 
						 v_bk_nf(i).VAR03                 , 
						 v_bk_nf(i).VAR04                 , 
						 v_bk_nf(i).VAR05                 , 
						 v_bk_nf(i).MNFST_IND_CNV115      , 
						 v_bk_nf(i).CNPJ_CPF              , 
						 v_bk_nf(i).MNFST_VAL_BASICMS     , 
						 v_bk_nf(i).MNFST_VAL_ICMS        , 
						 v_bk_nf(i).MNFST_VAL_ISENTAS     , 
						 v_bk_nf(i).MNFST_VAL_OUTRAS      , 
						 v_bk_nf(i).MNFST_CODH_NF         , 
						 v_bk_nf(i).MNFST_CODH_REGNF      , 
						 v_bk_nf(i).MNFST_CODH_REGCLI     , 
						 v_bk_nf(i).MNFST_REG_ESP         , 
						 v_bk_nf(i).MNFST_BAS_ICMS_ST     , 
						 v_bk_nf(i).MNFST_VAL_ICMS_ST     , 
						 v_bk_nf(i).MNFST_VAL_PIS         , 
						 v_bk_nf(i).MNFST_VAL_COFINS      , 
						 v_bk_nf(i).MNFST_VAL_DA          , 
						 v_bk_nf(i).MNFST_VAL_SER         , 
						 v_bk_nf(i).MNFST_VAL_TERC        , 
						 v_bk_nf(i).CICD_COD_INF          , 
						 v_bk_nf(i).MNFST_TIP_ASSI        , 
						 v_bk_nf(i).MNFST_TIP_UTIL        , 
						 v_bk_nf(i).MNFST_GRP_TENS        , 
						 v_bk_nf(i).MNFST_IND_EXTEMP      , 
						 v_bk_nf(i).MNFST_DAT_EXTEMP      , 
						 v_bk_nf(i).MNFST_NUM_FIC         , 
						 v_bk_nf(i).MNFST_DT_LT_ANT       , 
						 v_bk_nf(i).MNFST_DT_LT_ATU       , 
						 v_bk_nf(i).MNFST_NUM_FAT         , 
						 v_bk_nf(i).MNFST_VL_TOT_FAT      , 
						 v_bk_nf(i).MNFST_CHV_NFE         , 
						 v_bk_nf(i).MNFST_DAT_AUT_NFE     , 
						 v_bk_nf(i).MNFST_VAL_DESC_PIS    , 
						 v_bk_nf(i).MNFST_VAL_DESC_COFINS ,
						 v_bk_nf(i).MNFST_FIN_DOCE        ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
						 v_bk_nf(i).MNFST_CHV_REF         ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
						 v_bk_nf(i).MNFST_IND_DEST           -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
						 );
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
		  EXIT WHEN c_nf%NOTFOUND;	  
	   END LOOP;
	   CLOSE c_nf;
   END IF;
   
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_atu_nf);

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
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_nf,0) + :v_qtd_atu_nf
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

