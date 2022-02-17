#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}.log 2>> ${SCRIPT}_${PARTICAO_NF}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_INSERIR_ITENS'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
var v_qtd_atu_inf         NUMBER = 0
var v_qtd_atu_cli         NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_reg_paralizacao NUMBER = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_INSERIR_ITENS
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
   
   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   CURSOR c_inf
       IS
	SELECT  /*+ parallel(15) */
	 inf.*  
    FROM  openrisow.item_nftl_serv@gfprod inf,         
          openrisow.mestre_nftl_serv      PARTITION (${PARTICAO_NF})  nf
    WHERE ${FILTRO}
          AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
	  AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
        	AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2016','DD/MM/YYYY')		  
          AND inf.emps_cod = nf.emps_cod
          AND inf.fili_cod = nf.fili_cod
          AND inf.infst_serie = nf.mnfst_serie
          AND inf.infst_num = nf.mnfst_num
          AND inf.infst_dtemiss = nf.mnfst_dtemiss
          AND inf.mdoc_cod = nf.mdoc_cod
    ORDER BY nf.rowid;
   TYPE t_inf IS TABLE OF c_inf%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_inf t_inf;
   
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
   prc_tempo('inicio');  
   OPEN c_inf;
   LOOP
      FETCH c_inf BULK COLLECT INTO v_bk_inf LIMIT CONSTANTE_LIMIT;   
	  :v_qtd_atu_inf       := :v_qtd_atu_inf + v_bk_inf.COUNT;
      IF v_bk_inf.COUNT > 0 THEN
		FORALL i IN v_bk_inf.FIRST .. v_bk_inf.LAST
          INSERT INTO OPENRISOW.Item_NFTL_SERV INF (EMPS_COD     , 
                                           FILI_COD              , 
                                           CGC_CPF               , 
                                           IE                    , 
                                           UF                    , 
                                           TP_LOC                , 
                                           LOCALIDADE            , 
                                           TDOC_COD              , 
                                           INFST_SERIE           , 
                                           INFST_NUM             , 
                                           INFST_DTEMISS         , 
                                           CATG_COD              , 
                                           CADG_COD              , 
                                           SERV_COD              , 
                                           ESTB_COD              , 
                                           INFST_DSC_COMPL       , 
                                           INFST_VAL_CONT        , 
                                           INFST_VAL_SERV        , 
                                           INFST_VAL_DESC        , 
                                           INFST_ALIQ_ICMS       , 
                                           INFST_BASE_ICMS       , 
                                           INFST_VAL_ICMS        , 
                                           INFST_ISENTA_ICMS     , 
                                           INFST_OUTRAS_ICMS     , 
                                           INFST_TRIBIPI         , 
                                           INFST_TRIBICMS        , 
                                           INFST_ISENTA_IPI      , 
                                           INFST_OUTRA_IPI       , 
                                           INFST_OUTRAS_DESP     , 
                                           INFST_FISCAL          , 
                                           INFST_NUM_SEQ         , 
                                           INFST_TEL             , 
                                           INFST_IND_CANC        , 
                                           INFST_PROTER          , 
                                           INFST_COD_CONT        , 
                                           CFOP                  , 
                                           MDOC_COD              , 
                                           COD_PREST             , 
                                           NUM01                 , 
                                           NUM02                 , 
                                           NUM03                 , 
                                           VAR01                 , 
                                           VAR02                 , 
                                           VAR03                 , 
                                           VAR04                 , 
                                           VAR05                 , 
                                           INFST_IND_CNV115      , 
                                           INFST_UNID_MEDIDA     , 
                                           INFST_QUANT_CONTR     , 
                                           INFST_QUANT_PREST     , 
                                           INFST_CODH_REG        , 
                                           ESTA_COD              , 
                                           INFST_VAL_PIS         , 
                                           INFST_VAL_COFINS      , 
                                           INFST_BAS_ICMS_ST     , 
                                           INFST_ALIQ_ICMS_ST    , 
                                           INFST_VAL_ICMS_ST     , 
                                           INFST_VAL_RED         , 
                                           TPIS_COD              , 
                                           TCOF_COD              , 
                                           INFST_BAS_PISCOF      , 
                                           INFST_ALIQ_PIS        , 
                                           INFST_ALIQ_COFINS     , 
                                           INFST_NAT_REC         , 
                                           CSCP_COD              , 
                                           INFST_NUM_CONTR       , 
                                           INFST_TIP_ISENCAO     , 
                                           INFST_TAR_APLIC       , 
                                           INFST_IND_DESC        , 
                                           INFST_NUM_FAT         , 
                                           INFST_QTD_FAT         , 
                                           INFST_MOD_ATIV        , 
                                           INFST_HORA_ATIV       , 
                                           INFST_ID_EQUIP        , 
                                           INFST_MOD_PGTO        , 
                                           INFST_NUM_NFE         , 
                                           INFST_DTEMISS_NFE     , 
                                           INFST_VAL_CRED_NFE    , 
                                           INFST_CNPJ_CAN_COM    , 
                                           INFST_VAL_DESC_PIS    , 
                                           INFST_VAL_DESC_COFINS , 
                                           INFST_FCP_PRO         ,   -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                           INFST_FCP_ST              -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                           )

                        VALUES (v_bk_inf(i).EMPS_COD              , 
                                v_bk_inf(i).FILI_COD              , 
                                v_bk_inf(i).CGC_CPF               , 
                                v_bk_inf(i).IE                    , 
                                v_bk_inf(i).UF                    , 
                                v_bk_inf(i).TP_LOC                , 
                                v_bk_inf(i).LOCALIDADE            , 
                                v_bk_inf(i).TDOC_COD              , 
                                v_bk_inf(i).INFST_SERIE           , 
                                v_bk_inf(i).INFST_NUM             , 
                                v_bk_inf(i).INFST_DTEMISS         , 
                                v_bk_inf(i).CATG_COD              , 
                                v_bk_inf(i).CADG_COD              , 
                                v_bk_inf(i).SERV_COD              , 
                                v_bk_inf(i).ESTB_COD              , 
                                v_bk_inf(i).INFST_DSC_COMPL       , 
                                v_bk_inf(i).INFST_VAL_CONT        , 
                                v_bk_inf(i).INFST_VAL_SERV        , 
                                v_bk_inf(i).INFST_VAL_DESC        , 
                                v_bk_inf(i).INFST_ALIQ_ICMS       , 
                                v_bk_inf(i).INFST_BASE_ICMS       , 
                                v_bk_inf(i).INFST_VAL_ICMS        , 
                                v_bk_inf(i).INFST_ISENTA_ICMS     , 
                                v_bk_inf(i).INFST_OUTRAS_ICMS     , 
                                v_bk_inf(i).INFST_TRIBIPI         , 
                                v_bk_inf(i).INFST_TRIBICMS        , 
                                v_bk_inf(i).INFST_ISENTA_IPI      , 
                                v_bk_inf(i).INFST_OUTRA_IPI       , 
                                v_bk_inf(i).INFST_OUTRAS_DESP     , 
                                v_bk_inf(i).INFST_FISCAL          , 
                                v_bk_inf(i).INFST_NUM_SEQ         , 
                                v_bk_inf(i).INFST_TEL             , 
                                v_bk_inf(i).INFST_IND_CANC        , 
                                v_bk_inf(i).INFST_PROTER          , 
                                v_bk_inf(i).INFST_COD_CONT        , 
                                v_bk_inf(i).CFOP                  , 
                                v_bk_inf(i).MDOC_COD              , 
                                v_bk_inf(i).COD_PREST             , 
                                v_bk_inf(i).NUM01                 , 
                                v_bk_inf(i).NUM02                 , 
                                v_bk_inf(i).NUM03                 , 
                                v_bk_inf(i).VAR01                 , 
                                v_bk_inf(i).VAR02                 , 
                                v_bk_inf(i).VAR03                 , 
                                v_bk_inf(i).VAR04                 , 
                                v_bk_inf(i).VAR05                 , 
                                v_bk_inf(i).INFST_IND_CNV115      , 
                                v_bk_inf(i).INFST_UNID_MEDIDA     , 
                                v_bk_inf(i).INFST_QUANT_CONTR     , 
                                v_bk_inf(i).INFST_QUANT_PREST     , 
                                v_bk_inf(i).INFST_CODH_REG        , 
                                v_bk_inf(i).ESTA_COD              , 
                                v_bk_inf(i).INFST_VAL_PIS         , 
                                v_bk_inf(i).INFST_VAL_COFINS      , 
                                v_bk_inf(i).INFST_BAS_ICMS_ST     , 
                                v_bk_inf(i).INFST_ALIQ_ICMS_ST    , 
                                v_bk_inf(i).INFST_VAL_ICMS_ST     , 
                                v_bk_inf(i).INFST_VAL_RED         , 
                                v_bk_inf(i).TPIS_COD              , 
                                v_bk_inf(i).TCOF_COD              , 
                                v_bk_inf(i).INFST_BAS_PISCOF      , 
                                v_bk_inf(i).INFST_ALIQ_PIS        , 
                                v_bk_inf(i).INFST_ALIQ_COFINS     , 
                                v_bk_inf(i).INFST_NAT_REC         , 
                                v_bk_inf(i).CSCP_COD              , 
                                v_bk_inf(i).INFST_NUM_CONTR       , 
                                v_bk_inf(i).INFST_TIP_ISENCAO     , 
                                v_bk_inf(i).INFST_TAR_APLIC       , 
                                v_bk_inf(i).INFST_IND_DESC        , 
                                v_bk_inf(i).INFST_NUM_FAT         , 
                                v_bk_inf(i).INFST_QTD_FAT         , 
                                v_bk_inf(i).INFST_MOD_ATIV        , 
                                v_bk_inf(i).INFST_HORA_ATIV       , 
                                v_bk_inf(i).INFST_ID_EQUIP        , 
                                v_bk_inf(i).INFST_MOD_PGTO        , 
                                v_bk_inf(i).INFST_NUM_NFE         , 
                                v_bk_inf(i).INFST_DTEMISS_NFE     , 
                                v_bk_inf(i).INFST_VAL_CRED_NFE    , 
                                v_bk_inf(i).INFST_CNPJ_CAN_COM    , 
                                v_bk_inf(i).INFST_VAL_DESC_PIS    , 
                                v_bk_inf(i).INFST_VAL_DESC_COFINS , 
                                v_bk_inf(i).INFST_FCP_PRO         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                v_bk_inf(i).INFST_FCP_ST            -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                );	
      END IF;
	  ${COMMIT};	
	  EXIT WHEN c_inf%NOTFOUND;	  
   END LOOP;
   CLOSE c_inf;
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_atu_inf);
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
      :v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || :v_msg_erro,1,4000);
      :v_st_processamento := 'Erro';
      :exit_code := 1;
END;
/

PROMPT Processado
ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc = SYSDATE,
       cp.st_processamento = :v_st_processamento,
       cp.ds_msg_erro = substr(substr(nvl(:v_msg_erro,' '),1,1000) || cp.ds_msg_erro ,1,4000),
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_nf,0) + :v_qtd_atu_nf,
       cp.qt_atualizados_inf = NVL(cp.qt_atualizados_inf,0) + :v_qtd_atu_inf,
       cp.qt_atualizados_cli = NVL(cp.qt_atualizados_cli,0) + :v_qtd_atu_cli,
       cp.qt_atualizados_comp = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp
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

