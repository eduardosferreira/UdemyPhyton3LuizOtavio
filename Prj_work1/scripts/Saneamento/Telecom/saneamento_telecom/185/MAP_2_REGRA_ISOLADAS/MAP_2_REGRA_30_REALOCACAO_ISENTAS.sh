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
var v_st_processamento    VARCHAR2(50)   = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_30_REALOCACAO_ISENTAS'
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
PROMPT MAP_2_REGRA_30_REALOCACAO_ISENTAS
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE   

    CURSOR c_inf
       IS
	SELECT  /*+ parallel(15) */
			CASE lead(nf.rowid, 1) over (order by nf.rowid)
			  WHEN nf.rowid THEN 'N'
			  ELSE 'S'
			END last_ref_nf,  
			nf.rowid rowid_nf,
			nf.cnpj_cpf,
			nf.mnfst_num,
			inf.rowid             rowid_inf,         
			MAX(inf.infst_num_seq) OVER ( PARTITION BY nf.rowid ) infst_num_seq_max,  
		    to_NUMBER(0) update_reg,
			inf.*
	FROM    openrisow.item_nftl_serv      PARTITION (${PARTICAO_INF}) inf,  
			openrisow.mestre_nftl_serv    PARTITION (${PARTICAO_NF})  nf   
	WHERE   ${FILTRO}
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) IN ('UT')  
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
		AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
		  --AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2017','DD/MM/YYYY')	
		  AND inf.emps_cod                                               = nf.emps_cod
		  AND inf.fili_cod                                               = nf.fili_cod
		  AND inf.infst_serie                                            = nf.mnfst_serie
		  AND inf.infst_num                                              = nf.mnfst_num
		  AND inf.infst_dtemiss                                          = nf.mnfst_dtemiss
		  AND inf.mdoc_cod                                               = nf.mdoc_cod	  
		  AND EXISTS (SELECT 1 FROM  openrisow.item_nftl_serv    PARTITION (${PARTICAO_INF}) inf1 
					  WHERE inf1.emps_cod                                               = nf.emps_cod
					  AND   inf1.fili_cod                                               = nf.fili_cod
					  AND   inf1.infst_serie                                            = nf.mnfst_serie
					  AND   inf1.infst_num                                              = nf.mnfst_num
					  AND   inf1.infst_dtemiss                                          = nf.mnfst_dtemiss
					  AND   inf1.mdoc_cod                                               = nf.mdoc_cod
					  AND  NVL(inf1.INFST_OUTRAS_ICMS,0) = 0
				      AND  NVL(inf1.INFST_BASE_ICMS,0)   <> 0
					  AND  NVL(inf1.INFST_ISENTA_ICMS,0) <> 0					  
					  AND  NVL(inf1.INFST_VAL_DESC,0)     = 0
					  AND  NVL(inf1.INFST_VAL_SERV,0)     = NVL(inf1.INFST_VAL_CONT,0)	
					)    
    ORDER BY rowid_nf,   last_ref_nf ;
    v_inf                 c_inf%ROWTYPE;
    v_item_nftl_serv      c_inf%ROWTYPE;	
	
	v_rowid_nf            ROWID:=NULL;
	v_infst_num_seq_max   openrisow.item_nftl_serv.infst_num_seq%TYPE := NULL;
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

   prc_tempo('INICIO');
  

   
   OPEN c_inf;
   LOOP
    FETCH c_inf INTO v_inf;
    EXIT WHEN c_inf%NOTFOUND;

	-- Inicializacoes  
	:v_qtd_processados := :v_qtd_processados+1;
	v_inf.update_reg   := 0;
	
	if ( v_rowid_nf is null ) or (v_rowid_nf != v_inf.rowid_nf) then
	   v_rowid_nf                     := v_inf.rowid_nf;
	   v_infst_num_seq_max            := v_inf.infst_num_seq_max ;
	end if;	
    v_item_nftl_serv                  := v_inf;	
	v_item_nftl_serv.update_reg       := 0;
	
	IF NVL(v_item_nftl_serv.INFST_OUTRAS_ICMS,0) = 0
				      AND  NVL(v_item_nftl_serv.INFST_BASE_ICMS,0)   <> 0
					  AND  NVL(v_item_nftl_serv.INFST_ISENTA_ICMS,0) <> 0					  
					  AND  NVL(v_item_nftl_serv.INFST_VAL_DESC,0)     = 0
					  AND  NVL(v_item_nftl_serv.INFST_VAL_SERV,0)     = NVL(v_item_nftl_serv.INFST_VAL_CONT,0)				
	THEN		
		v_item_nftl_serv.VAR05             := SUBSTR('r2015_30i:' || v_item_nftl_serv.CFOP || '|' || v_item_nftl_serv.infst_val_cont || '|' || v_item_nftl_serv.INFST_VAL_SERV || '|' || v_item_nftl_serv.INFST_BASE_ICMS || '|' || v_item_nftl_serv.INFST_VAL_ICMS || '|' || v_item_nftl_serv.ESTB_COD || '|' || v_item_nftl_serv.INFST_ALIQ_ICMS || '|' || v_item_nftl_serv.INFST_TRIBICMS || '|' || v_item_nftl_serv.infst_val_red || '>>'|| v_item_nftl_serv.VAR05,1,150);
        v_item_nftl_serv.INFST_VAL_CONT   := v_item_nftl_serv.INFST_ISENTA_ICMS;
		v_item_nftl_serv.INFST_VAL_SERV   := v_item_nftl_serv.INFST_ISENTA_ICMS;
		v_item_nftl_serv.INFST_BASE_ICMS  := 0;
		v_item_nftl_serv.INFST_VAL_ICMS   := 0;
		v_item_nftl_serv.ESTB_COD         := '40';
		v_item_nftl_serv.INFST_ALIQ_ICMS  := 0;--25;
		v_item_nftl_serv.INFST_TRIBICMS   := 'N';
		v_item_nftl_serv.update_reg       := 1;
		v_item_nftl_serv.infst_val_red    := 0;
	END IF;
	
	IF v_item_nftl_serv.update_reg     = 1 THEN
	
	    v_infst_num_seq_max            := NVL(v_infst_num_seq_max,v_item_nftl_serv.infst_num_seq_max) + 1;
		v_item_nftl_serv.INFST_NUM_SEQ := v_infst_num_seq_max;
		INSERT INTO openrisow.item_nftl_serv 
		(
			EMPS_COD              , 
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
			infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			infst_fcp_st	         -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185  	  
		) 
		VALUES 
		(
			v_item_nftl_serv.EMPS_COD              , 
			v_item_nftl_serv.FILI_COD              , 
			v_item_nftl_serv.CGC_CPF               , 
			v_item_nftl_serv.IE                    , 
			v_item_nftl_serv.UF                    , 
			v_item_nftl_serv.TP_LOC                , 
			v_item_nftl_serv.LOCALIDADE            , 
			v_item_nftl_serv.TDOC_COD              , 
			v_item_nftl_serv.INFST_SERIE           , 
			v_item_nftl_serv.INFST_NUM             , 
			v_item_nftl_serv.INFST_DTEMISS         , 
			v_item_nftl_serv.CATG_COD              , 
			v_item_nftl_serv.CADG_COD              , 
			v_item_nftl_serv.SERV_COD              , 
			v_item_nftl_serv.ESTB_COD              , 
			v_item_nftl_serv.INFST_DSC_COMPL       , 
			v_item_nftl_serv.INFST_VAL_CONT        , 
			v_item_nftl_serv.INFST_VAL_SERV        , 
			v_item_nftl_serv.INFST_VAL_DESC        , 
			v_item_nftl_serv.INFST_ALIQ_ICMS       , 
			v_item_nftl_serv.INFST_BASE_ICMS       , 
			v_item_nftl_serv.INFST_VAL_ICMS        , 
			v_item_nftl_serv.INFST_ISENTA_ICMS     , 
			v_item_nftl_serv.INFST_OUTRAS_ICMS     , 
			v_item_nftl_serv.INFST_TRIBIPI         , 
			v_item_nftl_serv.INFST_TRIBICMS        , 
			v_item_nftl_serv.INFST_ISENTA_IPI      , 
			v_item_nftl_serv.INFST_OUTRA_IPI       , 
			v_item_nftl_serv.INFST_OUTRAS_DESP     , 
			v_item_nftl_serv.INFST_FISCAL          , 
			v_item_nftl_serv.INFST_NUM_SEQ         , 
			v_item_nftl_serv.INFST_TEL             , 
			v_item_nftl_serv.INFST_IND_CANC        , 
			v_item_nftl_serv.INFST_PROTER          , 
			v_item_nftl_serv.INFST_COD_CONT        , 
			v_item_nftl_serv.CFOP                  , 
			v_item_nftl_serv.MDOC_COD              , 
			v_item_nftl_serv.COD_PREST             , 
			v_item_nftl_serv.NUM01                 , 
			v_item_nftl_serv.NUM02                 , 
			v_item_nftl_serv.NUM03                 , 
			v_item_nftl_serv.VAR01                 , 
			v_item_nftl_serv.VAR02                 , 
			v_item_nftl_serv.VAR03                 , 
			v_item_nftl_serv.VAR04                 , 
			v_item_nftl_serv.VAR05                 , 
			v_item_nftl_serv.INFST_IND_CNV115      , 
			v_item_nftl_serv.INFST_UNID_MEDIDA     , 
			v_item_nftl_serv.INFST_QUANT_CONTR     , 
			v_item_nftl_serv.INFST_QUANT_PREST     , 
			v_item_nftl_serv.INFST_CODH_REG        , 
			v_item_nftl_serv.ESTA_COD              , 
			v_item_nftl_serv.INFST_VAL_PIS         , 
			v_item_nftl_serv.INFST_VAL_COFINS      , 
			v_item_nftl_serv.INFST_BAS_ICMS_ST     , 
			v_item_nftl_serv.INFST_ALIQ_ICMS_ST    , 
			v_item_nftl_serv.INFST_VAL_ICMS_ST     , 
			v_item_nftl_serv.INFST_VAL_RED         , 
			v_item_nftl_serv.TPIS_COD              , 
			v_item_nftl_serv.TCOF_COD              , 
			v_item_nftl_serv.INFST_BAS_PISCOF      , 
			v_item_nftl_serv.INFST_ALIQ_PIS        , 
			v_item_nftl_serv.INFST_ALIQ_COFINS     , 
			v_item_nftl_serv.INFST_NAT_REC         , 
			v_item_nftl_serv.CSCP_COD              , 
			v_item_nftl_serv.INFST_NUM_CONTR       , 
			v_item_nftl_serv.INFST_TIP_ISENCAO     , 
			v_item_nftl_serv.INFST_TAR_APLIC       , 
			v_item_nftl_serv.INFST_IND_DESC        , 
			v_item_nftl_serv.INFST_NUM_FAT         , 
			v_item_nftl_serv.INFST_QTD_FAT         , 
			v_item_nftl_serv.INFST_MOD_ATIV        , 
			v_item_nftl_serv.INFST_HORA_ATIV       , 
			v_item_nftl_serv.INFST_ID_EQUIP        , 
			v_item_nftl_serv.INFST_MOD_PGTO        , 
			v_item_nftl_serv.INFST_NUM_NFE         , 
			v_item_nftl_serv.INFST_DTEMISS_NFE     , 
			v_item_nftl_serv.INFST_VAL_CRED_NFE    , 
			v_item_nftl_serv.INFST_CNPJ_CAN_COM    , 
			v_item_nftl_serv.INFST_VAL_DESC_PIS    , 
			v_item_nftl_serv.INFST_VAL_DESC_COFINS ,
			v_item_nftl_serv.infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			v_item_nftl_serv.infst_fcp_st	         				 -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	 			  
		);
		:v_qtd_ins_inf                        := :v_qtd_ins_inf + 1;

	    -- Altera o registro original
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_30u:' || v_inf.INFST_ISENTA_ICMS || '|' || v_inf.infst_val_cont || '|' || v_inf.INFST_VAL_SERV || '|' || v_inf.infst_val_red || '>>'|| v_inf.VAR05,1,150);
        v_inf.INFST_ISENTA_ICMS    			  := 0;
        v_inf.INFST_VAL_SERV      			  := v_inf.INFST_BASE_ICMS;
        v_inf.INFST_VAL_CONT      			  := v_inf.INFST_BASE_ICMS;		
		v_inf.infst_val_red       		      := 0;
	
    END IF;	
			
	IF v_inf.update_reg = 1 THEN

	  UPDATE openrisow.item_nftl_serv inf
		SET inf.var05              = substr(v_inf.VAR05,1,150) 	 			
		  , inf.INFST_ISENTA_ICMS  = v_inf.INFST_ISENTA_ICMS
		  , inf.infst_val_serv     = v_inf.infst_val_serv 
		  , inf.infst_val_cont     = v_inf.infst_val_cont
		  , inf.infst_val_cont     = v_inf.infst_val_cont
		  , inf.infst_val_red      = v_inf.infst_val_red
	  WHERE rowid = v_inf.rowid_inf;
	  v_inf.update_reg   := 0;
	  :v_qtd_atu_inf     := :v_qtd_atu_inf + 1;
	  
	END IF;	

	IF v_inf.last_ref_nf  = 'S' THEN 	
		${COMMIT};
	END IF;
			
   END LOOP;
   CLOSE c_inf;
   
   ${COMMIT};
   prc_tempo('FIM');
   prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> NF : ' || :v_qtd_atu_nf || ' >> INF : ' || :v_qtd_atu_inf || ' >> INSERT INF : ' || :v_qtd_ins_inf);

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500) || ' - rowid_inf >> ' || v_inf.rowid_inf);
      :v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || :v_msg_erro,1,4000);
      :v_st_processamento := 'Erro';
      :exit_code := 1;
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

