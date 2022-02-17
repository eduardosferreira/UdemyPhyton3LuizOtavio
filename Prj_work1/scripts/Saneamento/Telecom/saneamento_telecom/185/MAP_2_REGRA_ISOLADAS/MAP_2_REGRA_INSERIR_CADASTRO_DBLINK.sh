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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_INSERIR_CADASTRO_DBLINK'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_INSERIR_CADASTRO_DBLINK
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
   v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_INSERIR_CADASTRO_DBLINK',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
   
   l_error_count  NUMBER;    
   ex_dml_errors EXCEPTION;
   PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);

   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   v_mnfst_dtemiss  openrisow.mestre_nftl_serv.mnfst_dtemiss%type;
   
   CURSOR c_nf(p_mnfst_dtemiss in openrisow.mestre_nftl_serv.mnfst_dtemiss%type)
       IS
	WITH TMP_CLI AS (SELECT DISTINCT  rowid_cli  
					 FROM 
					 (SELECT  
						 cli.rowid rowid_cli  ,
						 ROW_NUMBER() OVER (PARTITION BY nf.rowid ORDER BY cli.cadg_dat_atua DESC) rnk
						FROM  openrisow.mestre_nftl_serv${DBLINK1}   nf,
							  openrisow.cli_fornec_transp${DBLINK1}  cli  
						WHERE  ${FILTRO} 
						AND nf.mnfst_dtemiss BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY') 
						AND cli.cadg_cod       = nf.cadg_cod
						AND cli.catg_cod       = nf.catg_cod
						AND cli.cadg_dat_atua <= nf.mnfst_dtemiss) WHERE rnk = 1
					GROUP BY rowid_cli
					)	
	SELECT /*+ PARALLEL (8,8) */
		   cli.*                 ,
		   comp.rowid rowid_comp ,
           comp.CADG_TIP_ASSIN   ,
           comp.CADG_TIP_UTILIZ  ,
           comp.CADG_GRP_TENSAO  ,
           comp.CADG_TEL_CONTATO ,
           comp.CADG_NUM_CONTA   ,
           comp.CADG_UF_HABILIT  ,
           comp.CADG_TIP_CLI     ,
           comp.CADG_SUB_CONSU   ,
           comp.NUM01          as NUM01_COMP  ,
           comp.NUM02          as NUM02_COMP  ,
           comp.NUM03          as NUM03_COMP  ,
           comp.VAR01          as VAR01_COMP  ,
           comp.VAR02          as VAR02_COMP  ,
           comp.VAR03          as VAR03_COMP  ,
           comp.VAR04          as VAR04_COMP  ,
           comp.VAR05          as VAR05_COMP  	   
    FROM   TMP_CLI tmp ,
	       openrisow.cli_fornec_transp${DBLINK1}  cli,
		   openrisow.complvu_clifornec${DBLINK1}  comp
	WHERE  cli.rowid           = tmp.rowid_cli
	AND comp.cadg_cod          = cli.cadg_cod
	AND comp.catg_cod          = cli.catg_cod
	AND comp.cadg_dat_atua     = cli.cadg_dat_atua	
	;
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
   
   IF v_mnfst_dtemiss = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') THEN
	   prc_tempo('cursor');
	   OPEN c_nf(p_mnfst_dtemiss => v_mnfst_dtemiss);
	   LOOP
		  FETCH c_nf BULK COLLECT INTO v_bk_nf LIMIT CONSTANTE_LIMIT;   
		  :v_qtd_atu_nf       := :v_qtd_atu_nf + v_bk_nf.COUNT;
		  IF v_bk_nf.COUNT > 0 THEN
			BEGIN	
				FORALL i IN v_bk_nf.FIRST .. v_bk_nf.LAST SAVE EXCEPTIONS		
					INSERT INTO openrisow.cli_fornec_transp
					   (CADG_COD          ,
						CADG_DAT_ATUA     ,
						CATG_COD          ,
						PAIS_COD          ,
						UNFE_SIG          ,
						CADG_COD_CGCCPF   ,
						CADG_TIP          ,
						CADG_COD_INSEST   ,
						CADG_COD_INSMUN   ,
						EQUIPAR_RURAL     ,
						CADG_NOM          ,
						CADG_NOM_FANTASIA ,
						CADG_END          ,
						CADG_END_NUM      ,
						CADG_END_COMP     ,
						CADG_END_BAIRRO   ,
						CADG_END_MUNIC    ,
						CADG_END_CEP      ,
						CADG_IND_COLIGADA ,
						CADG_COD_SUFRAMA  ,
						TP_LOC            ,
						LOCA_COD          ,
						CADG_CEI          ,
						NUM01             ,
						NUM02             ,
						NUM03             ,
						VAR01             ,
						VAR02             ,
						VAR03             ,
						VAR04             ,
						VAR05             ,
						CADG_NIT          ,
						CADG_CX_POST      ,
						CADG_CEP_CXP      ,
						CADG_DDD_TEL      ,
						CADG_TEL          ,
						CADG_DDD_FAX      ,
						CADG_FAX          ,
						CADG_CLAS_RI      ,
						MIBGE_COD_MUN     ,
						CADG_DAT_LAUDO    ,
						CADG_IND_NIF      ,
						CADG_DSC_NIF      ,
						IREX_COD          ,
						CADG_IND_OB_CIVIL ,
						SIST_ORIGEM       ,
						USUA_ORIGEM       ,
						DATA_CRIACAO      ,
						ID_ORIGEM         ,
						FTRB_COD          ,
						CADG_INF_ISEN     ,	
						CADG_PROVINCIA    ) -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
					VALUES (v_bk_nf(i).CADG_COD          ,
						v_bk_nf(i).CADG_DAT_ATUA     ,
						v_bk_nf(i).CATG_COD          ,
						v_bk_nf(i).PAIS_COD          ,
						v_bk_nf(i).UNFE_SIG          ,
						v_bk_nf(i).CADG_COD_CGCCPF   ,
						v_bk_nf(i).CADG_TIP          ,
						v_bk_nf(i).CADG_COD_INSEST   ,
						v_bk_nf(i).CADG_COD_INSMUN   ,
						v_bk_nf(i).EQUIPAR_RURAL     ,
						v_bk_nf(i).CADG_NOM          ,
						v_bk_nf(i).CADG_NOM_FANTASIA ,
						v_bk_nf(i).CADG_END          ,
						v_bk_nf(i).CADG_END_NUM      ,
						v_bk_nf(i).CADG_END_COMP     ,
						v_bk_nf(i).CADG_END_BAIRRO   ,
						v_bk_nf(i).CADG_END_MUNIC    ,
						v_bk_nf(i).CADG_END_CEP      ,
						v_bk_nf(i).CADG_IND_COLIGADA ,
						v_bk_nf(i).CADG_COD_SUFRAMA  ,
						v_bk_nf(i).TP_LOC            ,
						v_bk_nf(i).LOCA_COD          ,
						v_bk_nf(i).CADG_CEI          ,
						v_bk_nf(i).NUM01             ,
						v_bk_nf(i).NUM02             ,
						v_bk_nf(i).NUM03             ,
						v_bk_nf(i).VAR01             ,
						v_bk_nf(i).VAR02             ,
						v_bk_nf(i).VAR03             ,
						v_bk_nf(i).VAR04             ,
						v_bk_nf(i).VAR05             ,
						v_bk_nf(i).CADG_NIT          ,
						v_bk_nf(i).CADG_CX_POST      ,
						v_bk_nf(i).CADG_CEP_CXP      ,
						v_bk_nf(i).CADG_DDD_TEL      ,
						v_bk_nf(i).CADG_TEL          ,
						v_bk_nf(i).CADG_DDD_FAX      ,
						v_bk_nf(i).CADG_FAX          ,
						v_bk_nf(i).CADG_CLAS_RI      ,
						v_bk_nf(i).MIBGE_COD_MUN     ,
						v_bk_nf(i).CADG_DAT_LAUDO    ,
						v_bk_nf(i).CADG_IND_NIF      ,
						v_bk_nf(i).CADG_DSC_NIF      ,
						v_bk_nf(i).IREX_COD          ,
						v_bk_nf(i).CADG_IND_OB_CIVIL ,
						v_bk_nf(i).SIST_ORIGEM       ,
						v_bk_nf(i).USUA_ORIGEM       ,
						v_bk_nf(i).DATA_CRIACAO      ,
						v_bk_nf(i).ID_ORIGEM         ,
						v_bk_nf(i).FTRB_COD          ,
						v_bk_nf(i).CADG_INF_ISEN     ,	
						v_bk_nf(i).CADG_PROVINCIA    );
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

			BEGIN	
				FORALL i IN v_bk_nf.FIRST .. v_bk_nf.LAST SAVE EXCEPTIONS		
					INSERT INTO openrisow.complvu_clifornec
						(   CADG_COD         ,
							CATG_COD         ,
							CADG_DAT_ATUA    ,
							CADG_TIP_ASSIN   ,
							CADG_TIP_UTILIZ  ,
							CADG_GRP_TENSAO  ,
							CADG_TEL_CONTATO ,
							CADG_NUM_CONTA   ,
							CADG_UF_HABILIT  ,
							CADG_TIP_CLI     ,
							CADG_SUB_CONSU   ,	
							NUM01  			 ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
							NUM02 			 ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
							NUM03            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
							VAR01            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	
							VAR02            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
							VAR03      		 ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
							VAR04            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	
							VAR05            )
					VALUES (v_bk_nf(i).CADG_COD         ,
							v_bk_nf(i).CATG_COD         ,
							v_bk_nf(i).CADG_DAT_ATUA    ,
							v_bk_nf(i).CADG_TIP_ASSIN   ,
							v_bk_nf(i).CADG_TIP_UTILIZ  ,
							v_bk_nf(i).CADG_GRP_TENSAO  ,
							v_bk_nf(i).CADG_TEL_CONTATO ,
							v_bk_nf(i).CADG_NUM_CONTA   ,
							v_bk_nf(i).CADG_UF_HABILIT  ,
							v_bk_nf(i).CADG_TIP_CLI     ,
							v_bk_nf(i).CADG_SUB_CONSU   ,	
							v_bk_nf(i).NUM01_COMP  			,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
							v_bk_nf(i).NUM02_COMP 			,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
							v_bk_nf(i).NUM03_COMP           ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
							v_bk_nf(i).VAR01_COMP           ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	
							v_bk_nf(i).VAR02_COMP           ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
							v_bk_nf(i).VAR03_COMP      		,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
							v_bk_nf(i).VAR04_COMP           ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	
							v_bk_nf(i).VAR05_COMP           );
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

