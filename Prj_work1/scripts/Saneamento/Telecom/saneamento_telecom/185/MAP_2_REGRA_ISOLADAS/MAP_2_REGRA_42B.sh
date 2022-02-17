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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_42B'
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
PROMPT MAP_2_REGRA_42B
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE   
 
	v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_42B',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32); 
	
    CURSOR c_inf
       IS
      SELECT /*+ parallel(8)*/
             inf.*
        FROM  openrisow.item_nftl_serv      PARTITION (${PARTICAO_INF}) inf 
			, openrisow.mestre_nftl_serv    PARTITION (${PARTICAO_NF})  nf   
	WHERE   ${FILTRO}
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1')
		  AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
	      AND inf.emps_cod                                               = nf.emps_cod
		  AND inf.fili_cod                                               = nf.fili_cod
		  AND inf.infst_serie                                            = nf.mnfst_serie
		  AND inf.infst_num                                              = nf.mnfst_num
		  AND inf.infst_dtemiss                                          = nf.mnfst_dtemiss
          AND inf.estb_cod             = '90'
          AND inf.infst_tribicms       = 'P'
          --AND inf.CFOP <> '0000'--REGEXP_LIKE(inf.CFOP, '^53|^63|^73')        
          AND inf.infst_val_cont       < 0
          AND inf.infst_val_serv       < 0
          AND inf.infst_outras_icms    < 0
          AND inf.infst_val_desc       = 0
          AND inf.infst_aliq_icms      = 0
          AND inf.infst_base_icms      = 0
          AND inf.infst_val_icms       = 0
          AND inf.infst_isenta_icms    = 0
         -- and infst_num = '000004064'
          AND EXISTS (SELECT /*+ first_rows(1)*/   1
                       FROM openrisow.ITEM_NFTL_SERV PARTITION (${PARTICAO_INF}) inf2
                      WHERE inf2.EMPS_COD            = inf.EMPS_COD
                        AND inf2.FILI_COD            = inf.FILI_COD
                        AND inf2.INFST_DTEMISS       = inf.INFST_DTEMISS
                        AND inf2.INFST_SERIE         = inf.INFST_SERIE
                        AND inf2.INFST_NUM           = inf.INFST_NUM
                        AND REPLACE(REPLACE(replace(replace(replace(replace(inf2.SERV_COD,REPLACE(inf2.INFST_SERIE,' ','')||'C08',''),REPLACE(inf2.INFST_SERIE,' ','')||'C09',''),REPLACE(inf2.INFST_SERIE,' ','')||'ZP',''),REPLACE(inf2.INFST_SERIE,' ','')||'ZN',''),REPLACE(inf2.INFST_SERIE,' ','')||'C',''),REPLACE(inf2.INFST_SERIE,' ','')||'L','') =  REPLACE(REPLACE(replace(replace(replace(replace(replace(inf.SERV_COD,REPLACE(inf.INFST_SERIE,' ','')||'C08',''),REPLACE(inf.INFST_SERIE,' ','')||'C09',''),REPLACE(inf.INFST_SERIE,' ','')||'ZP',''),REPLACE(inf.INFST_SERIE,' ','')||'ZN',''),REPLACE(inf.INFST_SERIE,' ','')||'C',''),REPLACE(inf.INFST_SERIE,' ','')||'C',''),REPLACE(inf.INFST_SERIE,' ','')||'L','')
						AND inf2.ESTB_COD            in('20')
                        --AND ABS(inf2.INFST_VAL_CONT) <> ABS(inf.INFST_VAL_CONT)
                        --AND ABS(inf2.INFST_VAL_SERV) <> ABS(inf.INFST_VAL_SERV)
                        AND inf2.INFST_VAL_CONT      > 0
                        AND inf2.INFST_VAL_SERV      > 0 
                        AND inf2.INFST_VAL_DESC      = 0
                        --AND inf2.INFST_ALIQ_ICMS     <> 0
                        AND inf2.INFST_BASE_ICMS     > 0
                        AND inf2.INFST_VAL_ICMS      > 0
                        AND (inf2.INFST_ISENTA_ICMS  > 0 
						AND inf2.INFST_ISENTA_ICMS >= ABS(inf.INFST_OUTRAS_ICMS))
                        AND inf2.INFST_OUTRAS_ICMS  = 0
                        and inf2.INFST_NUM_SEQ  = inf.INFST_NUM_SEQ - 1
                        )
      ORDER BY inf.INFST_NUM;
	  
    v_inf                 c_inf%ROWTYPE;
	v_infst_num_seq_new   openrisow.item_nftl_serv.infst_num_seq%type := 0;

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

   -----------------------------------------------------------------------------
   --> Nomeando o processo
   -----------------------------------------------------------------------------	
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);
	
   prc_tempo('INICIO');
  
   OPEN c_inf;
   LOOP
    FETCH c_inf INTO v_inf;
    EXIT WHEN c_inf%NOTFOUND;

	  -- Inicializacoes  
	  :v_qtd_processados      := :v_qtd_processados+1;
	  v_infst_num_seq_new     := 0;

      BEGIN
          SELECT /*+ first_rows(1)*/
                 max(inf.infst_num_seq) + 1
            INTO v_infst_num_seq_new
            FROM openrisow.ITEM_NFTL_SERV  PARTITION (${PARTICAO_INF}) inf 
           WHERE inf.EMPS_COD             =  v_inf.EMPS_COD
             AND inf.FILI_COD             =  v_inf.FILI_COD
             AND inf.INFST_DTEMISS        =  v_inf.INFST_DTEMISS
             AND inf.INFST_SERIE          =  v_inf.INFST_SERIE
             AND inf.INFST_NUM            =  v_inf.INFST_NUM;      
      EXCEPTION
        WHEN OTHERS THEN
          EXIT;
      END;
    
      INSERT INTO openrisow.ITEM_NFTL_SERV  VALUES ( v_inf.EMPS_COD,
                                                     v_inf.FILI_COD,
                                                     v_inf.CGC_CPF,
                                                     v_inf.IE,
                                                     v_inf.UF,
                                                     v_inf.TP_LOC,
                                                     v_inf.LOCALIDADE,
                                                     v_inf.TDOC_COD,
                                                     v_inf.INFST_SERIE,
                                                     v_inf.INFST_NUM,
                                                     v_inf.INFST_DTEMISS,
                                                     v_inf.CATG_COD,
                                                     v_inf.CADG_COD,
                                                     v_inf.SERV_COD,
                                                     v_inf.ESTB_COD,
                                                     v_inf.INFST_DSC_COMPL,
                                                     ABS(v_inf.INFST_VAL_CONT),
                                                     ABS(v_inf.INFST_VAL_SERV),
                                                     v_inf.INFST_VAL_DESC,
                                                     v_inf.INFST_ALIQ_ICMS,
                                                     v_inf.INFST_BASE_ICMS,
                                                     v_inf.INFST_VAL_ICMS,
                                                     v_inf.INFST_ISENTA_ICMS,
                                                     ABS(v_inf.INFST_OUTRAS_ICMS),
                                                     v_inf.INFST_TRIBIPI,
                                                     v_inf.INFST_TRIBICMS,
                                                     v_inf.INFST_ISENTA_IPI,
                                                     v_inf.INFST_OUTRA_IPI,
                                                     v_inf.INFST_OUTRAS_DESP,
                                                     v_inf.INFST_FISCAL,
                                                     v_infst_num_seq_new,
                                                     v_inf.INFST_TEL,
                                                     v_inf.INFST_IND_CANC,
                                                     v_inf.INFST_PROTER,
                                                     v_inf.INFST_COD_CONT,
                                                     v_inf.CFOP,
                                                     v_inf.MDOC_COD,
                                                     v_inf.COD_PREST,
                                                     v_inf.NUM01,
                                                     v_inf.NUM02,
                                                     v_inf.NUM03,
                                                     v_inf.VAR01,
                                                     v_inf.VAR02,
                                                     v_inf.VAR03,
                                                     v_inf.VAR04,
                                                     v_inf.VAR05,
                                                     v_inf.INFST_IND_CNV115,
                                                     v_inf.INFST_UNID_MEDIDA,
                                                     v_inf.INFST_QUANT_CONTR,
                                                     v_inf.INFST_QUANT_PREST,
                                                     v_inf.INFST_CODH_REG,
                                                     v_inf.ESTA_COD,
                                                     v_inf.INFST_VAL_PIS,
                                                     v_inf.INFST_VAL_COFINS,
                                                     v_inf.INFST_BAS_ICMS_ST,
                                                     v_inf.INFST_ALIQ_ICMS_ST,
                                                     v_inf.INFST_VAL_ICMS_ST,
                                                     v_inf.INFST_VAL_RED,
                                                     v_inf.TPIS_COD,
                                                     v_inf.TCOF_COD,
                                                     v_inf.INFST_BAS_PISCOF,
                                                     v_inf.INFST_ALIQ_PIS,
                                                     v_inf.INFST_ALIQ_COFINS,
                                                     v_inf.INFST_NAT_REC,
                                                     v_inf.CSCP_COD,
                                                     v_inf.INFST_NUM_CONTR,
                                                     v_inf.INFST_TIP_ISENCAO,
                                                     v_inf.INFST_TAR_APLIC,
                                                     v_inf.INFST_IND_DESC,
                                                     v_inf.INFST_NUM_FAT,
                                                     v_inf.INFST_QTD_FAT,
                                                     v_inf.INFST_MOD_ATIV,
                                                     v_inf.INFST_HORA_ATIV,
                                                     v_inf.INFST_ID_EQUIP,
                                                     v_inf.INFST_MOD_PGTO,
                                                     v_inf.INFST_NUM_NFE,
                                                     v_inf.INFST_DTEMISS_NFE,
                                                     v_inf.INFST_VAL_CRED_NFE,
                                                     v_inf.INFST_CNPJ_CAN_COM,
                                                     v_inf.INFST_VAL_DESC_PIS,
                                                     v_inf.INFST_VAL_DESC_COFINS,
													 v_inf.infst_fcp_pro        , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
													 v_inf.infst_fcp_st	         				 -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
													 );
                                                   
      UPDATE openrisow.ITEM_NFTL_SERV  PARTITION (${PARTICAO_INF}) inf2
         SET inf2.INFST_VAL_CONT      = inf2.INFST_VAL_CONT - ABS(v_inf.INFST_OUTRAS_ICMS),
             inf2.INFST_VAL_SERV      = inf2.INFST_VAL_SERV - ABS(v_inf.INFST_OUTRAS_ICMS),
             inf2.INFST_ISENTA_ICMS   = inf2.INFST_ISENTA_ICMS - ABS(v_inf.INFST_OUTRAS_ICMS),
			 inf2.estb_cod            = (CASE WHEN NVL(nvl(inf2.INFST_ISENTA_ICMS,0) - NVL(ABS(v_inf.INFST_OUTRAS_ICMS),0),0) <> 0 THEN '20' ELSE '00' END)
       WHERE inf2.EMPS_COD            = v_inf.EMPS_COD
         AND inf2.FILI_COD            = v_inf.FILI_COD
         AND inf2.INFST_DTEMISS       = v_inf.INFST_DTEMISS
         AND inf2.INFST_SERIE         = v_inf.INFST_SERIE
         AND inf2.INFST_NUM           = v_inf.INFST_NUM
         --AND inf2.SERV_COD            = v_inf.SERV_COD
		 AND REPLACE(REPLACE(replace(replace(replace(replace(inf2.SERV_COD,REPLACE(inf2.INFST_SERIE,' ','')||'C08',''),REPLACE(inf2.INFST_SERIE,' ','')||'C09',''),REPLACE(inf2.INFST_SERIE,' ','')||'ZP',''),REPLACE(inf2.INFST_SERIE,' ','')||'ZN',''),REPLACE(inf2.INFST_SERIE,' ','')||'C',''),REPLACE(inf2.INFST_SERIE,' ','')||'L','') =  REPLACE(REPLACE(replace(replace(replace(replace(v_inf.SERV_COD,REPLACE(v_inf.INFST_SERIE,' ','')||'C08',''),REPLACE(v_inf.INFST_SERIE,' ','')||'C09',''),REPLACE(v_inf.INFST_SERIE,' ','')||'ZP',''),REPLACE(v_inf.INFST_SERIE,' ','')||'ZN',''),REPLACE(v_inf.INFST_SERIE,' ','')||'C',''),REPLACE(v_inf.INFST_SERIE,' ','')||'L','')
         AND inf2.ESTB_COD            in('20')
         --AND ABS(inf2.INFST_VAL_CONT) <> ABS(v_inf.INFST_VAL_CONT)
         --AND ABS(inf2.INFST_VAL_SERV) <> ABS(v_inf.INFST_VAL_SERV)
         AND inf2.INFST_VAL_CONT      > 0
         AND inf2.INFST_VAL_SERV      > 0 
         AND inf2.INFST_VAL_DESC      = 0
         --AND inf2.INFST_ALIQ_ICMS     <> 0
         AND inf2.INFST_BASE_ICMS     > 0
         AND inf2.INFST_VAL_ICMS      > 0
         AND (inf2.INFST_ISENTA_ICMS  > 0 AND inf2.INFST_ISENTA_ICMS >= ABS(v_inf.INFST_OUTRAS_ICMS))
         AND inf2.INFST_OUTRAS_ICMS   = 0
         AND inf2.INFST_NUM_SEQ       = v_inf.INFST_NUM_SEQ - 1;  
		 :v_qtd_atu_inf := :v_qtd_atu_inf + 1;

   END LOOP;
   CLOSE c_inf;
   
   ${COMMIT};
   prc_tempo('FIM');
   prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> NF : ' || :v_qtd_atu_nf || ' >> INF : ' || :v_qtd_atu_inf || ' >> INSERT INF : ' || :v_qtd_ins_inf);
  
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
	  BEGIN
		DBMS_APPLICATION_INFO.set_module(null,null);
		DBMS_APPLICATION_INFO.set_client_info (null);		  
	  EXCEPTION
		WHEN OTHERS THEN
			NULL;
	  END;	
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

