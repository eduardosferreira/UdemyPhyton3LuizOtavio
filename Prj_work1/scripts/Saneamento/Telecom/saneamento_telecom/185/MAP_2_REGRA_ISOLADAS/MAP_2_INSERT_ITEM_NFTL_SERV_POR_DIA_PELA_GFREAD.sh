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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_INSERT_ITEM_NFTL_SERV_POR_DIA_PELA_GFREAD'
var exit_code             NUMBER         = 0
var v_qtd_atu             NUMBER         = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_INSERT_ITEM_NFTL_SERV_POR_DIA_PELA_GFREAD
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE   

	v_action_name VARCHAR2(32) := substr('MAP_2_INSERT_ITEM_NFTL_SERV_POR_DIA_PELA_GFREAD',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);

	l_error_count  NUMBER;    
    ex_dml_errors  EXCEPTION;
    PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
	v_error_bk     VARCHAR2(4000);
	
    CONSTANTE_LIMIT PLS_INTEGER := 500; 

    CURSOR c_sanea(p_dt in DATE)  
	IS 
	SELECT /*+ parallel(8,8) */ 
	*
	FROM     openrisow.item_nftl_serv@gfread   
	WHERE    fili_cod        in ('3506','3516','3003','3064','1710','1783')
	AND      emps_cod         = 'TBRA' 
	AND      infst_dtemiss    = p_dt;
		
    
	
	TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
	v_bk_sanea               t_sanea;
	v_sanea                  c_sanea%ROWTYPE;
	
	v_cp                         ${TABELA_CONTROLE}%ROWTYPE;
	v_ds_etapa                   VARCHAR2(4000);
    PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
	BEGIN
		BEGIN
		  v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		  DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
		EXCEPTION
		  WHEN OTHERS THEN
			NULL;
		END;
		DBMS_APPLICATION_INFO.set_client_info (substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,62));
	EXCEPTION
	  WHEN OTHERS THEN
		NULL;
	END;
	
	PROCEDURE prc_debug AS 
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		BEGIN
			UPDATE ${TABELA_CONTROLE} cp   SET cp.ds_msg_erro          = substr(v_ds_etapa ,1,4000)	 WHERE cp.rowid = '${ROWID_CP}';	
			DBMS_APPLICATION_INFO.set_client_info(substr(v_ds_etapa ,1,62));				 	
		EXCEPTION
		 WHEN OTHERS THEN
		   NULL;
		END;
		COMMIT;
	END;	
	
	    
BEGIN
    -----------------------------------------------------------------------------
    --> Nomeando o processo
    -----------------------------------------------------------------------------	
    DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
    DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);	
    prc_tempo('INICIO'); 

    SELECT *
    INTO   v_cp
    FROM   ${TABELA_CONTROLE} cp
    WHERE  cp.rowid = '${ROWID_CP}';
   
    IF v_cp.dt_limite_inf_nf = TO_DATE('01/01/2015','DD/MM/YYYY') THEN
		FOR C IN (SELECT * FROM (select  (TO_DATE('01/01/2015','DD/MM/YYYY')-1) + rownum DT_EXECUCAO  from DUAL connect by level  < 1830) WHERE DT_EXECUCAO < TO_DATE('01/01/2020','DD/MM/YYYY') ORDER BY DT_EXECUCAO)
		LOOP 	   
			prc_tempo(to_char(C.DT_EXECUCAO,'DD/MM/YYYY') || ' >> ' || TO_CHAR(:v_qtd_atu));
			prc_debug;
			OPEN c_sanea(p_dt => C.DT_EXECUCAO);
			LOOP
				FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
				prc_tempo(to_char(C.DT_EXECUCAO,'DD/MM/YYYY') || ' >> PROCESSAR: ' || TO_CHAR(v_bk_sanea.COUNT) || ' >> ' || TO_CHAR(:v_qtd_atu));
				IF v_bk_sanea.COUNT > 0 THEN
				BEGIN
					v_error_bk := NULL;
					FORALL i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST  SAVE EXCEPTIONS
						INSERT INTO openrisow.item_nftl_serv 
						(
							emps_cod              , 
							fili_cod              , 
							cgc_cpf               , 
							ie                    , 
							uf                    , 
							tp_loc                , 
							localidade            , 
							tdoc_cod              , 
							infst_serie           , 
							infst_num             , 
							infst_dtemiss         , 
							catg_cod              , 
							cadg_cod              , 
							serv_cod              , 
							estb_cod              , 
							infst_dsc_compl       , 
							infst_val_cont        , 
							infst_val_serv        , 
							infst_val_desc        , 
							infst_aliq_icms       , 
							infst_base_icms       , 
							infst_val_icms        , 
							infst_isenta_icms     , 
							infst_outras_icms     , 
							infst_tribipi         , 
							infst_tribicms        , 
							infst_isenta_ipi      , 
							infst_outra_ipi       , 
							infst_outras_desp     , 
							infst_fiscal          , 
							infst_num_seq         , 
							infst_tel             , 
							infst_ind_canc        , 
							infst_proter          , 
							infst_cod_cont        , 
							cfop                  , 
							mdoc_cod              , 
							cod_prest             , 
							num01                 , 
							num02                 , 
							num03                 , 
							var01                 , 
							var02                 , 
							var03                 , 
							var04                 , 
							var05                 , 
							infst_ind_cnv115      , 
							infst_unid_medida     , 
							infst_quant_contr     , 
							infst_quant_prest     , 
							infst_codh_reg        , 
							esta_cod              , 
							infst_val_pis         , 
							infst_val_cofins      , 
							infst_bas_icms_st     , 
							infst_aliq_icms_st    , 
							infst_val_icms_st     , 
							infst_val_red         , 
							tpis_cod              , 
							tcof_cod              , 
							infst_bas_piscof      , 
							infst_aliq_pis        , 
							infst_aliq_cofins     , 
							infst_nat_rec         , 
							cscp_cod              , 
							infst_num_contr       , 
							infst_tip_isencao     , 
							infst_tar_aplic       , 
							infst_ind_desc        , 
							infst_num_fat         , 
							infst_qtd_fat         , 
							infst_mod_ativ        , 
							infst_hora_ativ       , 
							infst_id_equip        , 
							infst_mod_pgto        , 
							infst_num_nfe         , 
							infst_dtemiss_nfe     , 
							infst_val_cred_nfe    , 
							infst_cnpj_can_com    , 
							infst_val_desc_pis    , 
							infst_val_desc_cofins ,
						    infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			                infst_fcp_st	        -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
						)
						VALUES
						(
							v_bk_sanea(i).emps_cod              , 
							v_bk_sanea(i).fili_cod              , 
							v_bk_sanea(i).cgc_cpf               , 
							v_bk_sanea(i).ie                    , 
							v_bk_sanea(i).uf                    , 
							v_bk_sanea(i).tp_loc                , 
							v_bk_sanea(i).localidade            , 
							v_bk_sanea(i).tdoc_cod              , 
							v_bk_sanea(i).infst_serie           , 
							v_bk_sanea(i).infst_num             , 
							v_bk_sanea(i).infst_dtemiss         , 
							v_bk_sanea(i).catg_cod              , 
							v_bk_sanea(i).cadg_cod              , 
							v_bk_sanea(i).serv_cod              , 
							v_bk_sanea(i).estb_cod              , 
							v_bk_sanea(i).infst_dsc_compl       , 
							v_bk_sanea(i).infst_val_cont        , 
							v_bk_sanea(i).infst_val_serv        , 
							v_bk_sanea(i).infst_val_desc        , 
							v_bk_sanea(i).infst_aliq_icms       , 
							v_bk_sanea(i).infst_base_icms       , 
							v_bk_sanea(i).infst_val_icms        , 
							v_bk_sanea(i).infst_isenta_icms     , 
							v_bk_sanea(i).infst_outras_icms     , 
							v_bk_sanea(i).infst_tribipi         , 
							v_bk_sanea(i).infst_tribicms        , 
							v_bk_sanea(i).infst_isenta_ipi      , 
							v_bk_sanea(i).infst_outra_ipi       , 
							v_bk_sanea(i).infst_outras_desp     , 
							v_bk_sanea(i).infst_fiscal          , 
							v_bk_sanea(i).infst_num_seq         , 
							v_bk_sanea(i).infst_tel             , 
							v_bk_sanea(i).infst_ind_canc        , 
							v_bk_sanea(i).infst_proter          , 
							v_bk_sanea(i).infst_cod_cont        , 
							v_bk_sanea(i).cfop                  , 
							v_bk_sanea(i).mdoc_cod              , 
							v_bk_sanea(i).cod_prest             , 
							v_bk_sanea(i).num01                 , 
							v_bk_sanea(i).num02                 , 
							v_bk_sanea(i).num03                 , 
							v_bk_sanea(i).var01                 , 
							v_bk_sanea(i).var02                 , 
							v_bk_sanea(i).var03                 , 
							v_bk_sanea(i).var04                 , 
							v_bk_sanea(i).var05                 , 
							v_bk_sanea(i).infst_ind_cnv115      , 
							v_bk_sanea(i).infst_unid_medida     , 
							v_bk_sanea(i).infst_quant_contr     , 
							v_bk_sanea(i).infst_quant_prest     , 
							v_bk_sanea(i).infst_codh_reg        , 
							v_bk_sanea(i).esta_cod              , 
							v_bk_sanea(i).infst_val_pis         , 
							v_bk_sanea(i).infst_val_cofins      , 
							v_bk_sanea(i).infst_bas_icms_st     , 
							v_bk_sanea(i).infst_aliq_icms_st    , 
							v_bk_sanea(i).infst_val_icms_st     , 
							v_bk_sanea(i).infst_val_red         , 
							v_bk_sanea(i).tpis_cod              , 
							v_bk_sanea(i).tcof_cod              , 
							v_bk_sanea(i).infst_bas_piscof      , 
							v_bk_sanea(i).infst_aliq_pis        , 
							v_bk_sanea(i).infst_aliq_cofins     , 
							v_bk_sanea(i).infst_nat_rec         , 
							v_bk_sanea(i).cscp_cod              , 
							v_bk_sanea(i).infst_num_contr       , 
							v_bk_sanea(i).infst_tip_isencao     , 
							v_bk_sanea(i).infst_tar_aplic       , 
							v_bk_sanea(i).infst_ind_desc        , 
							v_bk_sanea(i).infst_num_fat         , 
							v_bk_sanea(i).infst_qtd_fat         , 
							v_bk_sanea(i).infst_mod_ativ        , 
							v_bk_sanea(i).infst_hora_ativ       , 
							v_bk_sanea(i).infst_id_equip        , 
							v_bk_sanea(i).infst_mod_pgto        , 
							v_bk_sanea(i).infst_num_nfe         , 
							v_bk_sanea(i).infst_dtemiss_nfe     , 
							v_bk_sanea(i).infst_val_cred_nfe    , 
							v_bk_sanea(i).infst_cnpj_can_com    , 
							v_bk_sanea(i).infst_val_desc_pis    , 
							v_bk_sanea(i).infst_val_desc_cofins ,  
							v_bk_sanea(i).infst_fcp_pro         ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
							v_bk_sanea(i).infst_fcp_st             -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
						);
			
								
					:v_qtd_atu := :v_qtd_atu + v_bk_sanea.COUNT;
					${COMMIT};
					prc_tempo(to_char(C.DT_EXECUCAO,'DD/MM/YYYY') || ' >> : ' || TO_CHAR(:v_qtd_atu));
					prc_debug;
				EXCEPTION
				WHEN ex_dml_errors THEN
					BEGIN 
					  l_error_count := SQL%BULK_EXCEPTIONS.count;
					  prc_tempo(' >> ERRO : ' || TO_CHAR(l_error_count));
					  FOR i IN 1 .. l_error_count LOOP			
							IF -SQL%BULK_EXCEPTIONS(i).ERROR_CODE != -1 THEN
								v_error_bk    := SUBSTR('Error: ' || i ||  
														' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||  
														' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),1,500)
												 || ' | ' ||
												 SUBSTR(v_error_bk,1,3490);		
							END IF;				 
					  END LOOP;
					EXCEPTION
						WHEN OTHERS THEN
							v_error_bk    := NULL;
					END;
					IF NVL(LENGTH(TRIM(v_error_bk)),0) > 0 THEN
					  v_error_bk    := SUBSTR('Number of failures: ' || l_error_count,1,500)
										 || ' | ' ||
										 SUBSTR(v_error_bk,1,3490);
					  prc_tempo(v_error_bk);	
					  RAISE_APPLICATION_ERROR (-20343, 'STOP! ERRO INSERT ST >> ' || SUBSTR(v_error_bk,1,1000));	
					END IF;		  
				END;
				END IF;
				EXIT WHEN c_sanea%NOTFOUND;	  
			END LOOP; 	  
			CLOSE c_sanea; 	   
		
		END LOOP;   
    END IF;
    ${COMMIT};	
    prc_tempo('FIM');
    prc_tempo('Processados ${COMMIT} : ' || :v_qtd_atu);
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
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc          = SYSDATE,
       cp.st_processamento     = :v_st_processamento,
       cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0)   + :v_qtd_atu
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

