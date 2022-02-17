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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_COPIA_CLI_FORNEC_TRANSP'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_cli        NUMBER = 0
var v_qtd_atu_inf         NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_COPIA_CLI_FORNEC_TRANSP
PROMPT ### Inicio do processo ${0}  ###
PROMPT
DECLARE

	v_action_name VARCHAR2(32) := substr('MAP_2_COPIA_CLI_FORNEC_TRANSP',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);

	CONSTANTE_LIMIT PLS_INTEGER := 50000; 

	l_error_count  NUMBER;    
	ex_dml_errors  EXCEPTION;
	PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
	v_error_bk     VARCHAR2(4000);
	
    CURSOR c_sanea(p_dt IN DATE) 
		IS 
			SELECT /*+ PARALLEL (8,8) */  
				cli.CADG_COD          ,
				tmp.DT_PERIODO CADG_DAT_ATUA     ,
				cli.CATG_COD          ,
				cli.PAIS_COD          ,
				cli.UNFE_SIG          ,
				cli.CADG_COD_CGCCPF   ,
				cli.CADG_TIP          ,
				cli.CADG_COD_INSEST   ,
				cli.CADG_COD_INSMUN   ,
				cli.EQUIPAR_RURAL     ,
				cli.CADG_NOM          ,
				cli.CADG_NOM_FANTASIA ,
				cli.CADG_END          ,
				cli.CADG_END_NUM      ,
				cli.CADG_END_COMP     ,
				cli.CADG_END_BAIRRO   ,
				cli.CADG_END_MUNIC    ,
				cli.CADG_END_CEP      ,
				cli.CADG_IND_COLIGADA ,
				cli.CADG_COD_SUFRAMA  ,
				cli.TP_LOC            ,
				cli.LOCA_COD          ,
				cli.CADG_CEI          ,
				cli.NUM01             ,
				cli.NUM02             ,
				cli.NUM03             ,
				cli.VAR01             ,
				cli.VAR02             ,
				cli.VAR03             ,
				cli.VAR04             ,
				cli.VAR05             ,
				cli.CADG_NIT          ,
				cli.CADG_CX_POST      ,
				cli.CADG_CEP_CXP      ,
				cli.CADG_DDD_TEL      ,
				cli.CADG_TEL          ,
				cli.CADG_DDD_FAX      ,
				cli.CADG_FAX          ,
				cli.CADG_CLAS_RI      ,
				cli.MIBGE_COD_MUN     ,
				cli.CADG_DAT_LAUDO    ,
				cli.CADG_IND_NIF      ,
				cli.CADG_DSC_NIF      ,
				cli.IREX_COD          ,
				cli.CADG_IND_OB_CIVIL ,
				cli.SIST_ORIGEM       ,
				cli.USUA_ORIGEM       ,
				cli.DATA_CRIACAO      ,
				cli.ID_ORIGEM         ,
				cli.FTRB_COD          ,
				cli.CADG_INF_ISEN     ,
				cli.CADG_PROVINCIA		-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
            FROM
				OPENRISOW.CLI_FORNEC_TRANSP        cli,
                GFCADASTRO.TMP_CADASTRO_CLI_COMP   tmp
            WHERE
				cli.rowid = tmp.rowid_cli;
	
	-- TRUNC(cadg_dat_atua,'YYYY') = TRUNC(p_dt,'YYYY');
		
    TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
    v_bk_sanea t_sanea;
    v_sanea    c_sanea%ROWTYPE;

	v_dt_aux              DATE; 
    v_ds_etapa            VARCHAR2(4000);
    PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
    BEGIN
		v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
    EXCEPTION
    WHEN OTHERS THEN
	    NULL;
    END;

	PROCEDURE prcts_debug(p_ds_ddo IN VARCHAR2 := NULL) AS 		
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		BEGIN
			v_ds_etapa := substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' - > ' || p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		EXCEPTION
		 WHEN OTHERS THEN
		   NULL;
		END;
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

	-- Inicializacao
	prc_tempo('Inicializacao');

	-----------------------------------------------------------------------------
	--> Nomeando o processo
	-----------------------------------------------------------------------------	
	DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
	DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);

    SELECT cp.DT_LIMITE_INF_NF
    INTO   v_dt_aux
    FROM   ${TABELA_CONTROLE} cp
    WHERE  cp.rowid = '${ROWID_CP}';
   
    IF v_dt_aux = TRUNC(v_dt_aux,'YYYY') THEN 
		prc_tempo('SANEA');
		OPEN c_sanea(p_dt=>v_dt_aux);
		LOOP
			FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
			:v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
			IF v_bk_sanea.COUNT > 0 THEN
				BEGIN
					l_error_count :=0;
					v_error_bk := NULL;
					FORALL i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST SAVE EXCEPTIONS
						INSERT INTO OPENRISOW.CLI_FORNEC_TRANSP ( CADG_COD          ,  
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
                                                    CADG_PROVINCIA        -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                    )            
		                                          VALUES(v_bk_sanea(i).CADG_COD          ,  
                                                         v_bk_sanea(i).CADG_DAT_ATUA     ,  
                                                         v_bk_sanea(i).CATG_COD          ,  
                                                         v_bk_sanea(i).PAIS_COD          ,           
                                                         v_bk_sanea(i).UNFE_SIG          ,           
                                                         v_bk_sanea(i).CADG_COD_CGCCPF   ,           
                                                         v_bk_sanea(i).CADG_TIP          ,           
                                                         v_bk_sanea(i).CADG_COD_INSEST   ,           
                                                         v_bk_sanea(i).CADG_COD_INSMUN   ,           
                                                         v_bk_sanea(i).EQUIPAR_RURAL     ,           
                                                         v_bk_sanea(i).CADG_NOM          ,           
                                                         v_bk_sanea(i).CADG_NOM_FANTASIA ,           
                                                         v_bk_sanea(i).CADG_END          ,           
                                                         v_bk_sanea(i).CADG_END_NUM      ,           
                                                         v_bk_sanea(i).CADG_END_COMP     ,           
                                                         v_bk_sanea(i).CADG_END_BAIRRO   ,           
                                                         v_bk_sanea(i).CADG_END_MUNIC    ,           
                                                         v_bk_sanea(i).CADG_END_CEP      ,           
                                                         v_bk_sanea(i).CADG_IND_COLIGADA ,           
                                                         v_bk_sanea(i).CADG_COD_SUFRAMA  ,           
                                                         v_bk_sanea(i).TP_LOC            ,          
                                                         v_bk_sanea(i).LOCA_COD          ,          
                                                         v_bk_sanea(i).CADG_CEI          ,          
                                                         v_bk_sanea(i).NUM01             ,          
                                                         v_bk_sanea(i).NUM02             ,          
                                                         v_bk_sanea(i).NUM03             ,          
                                                         v_bk_sanea(i).VAR01             ,          
                                                         v_bk_sanea(i).VAR02             ,          
                                                         v_bk_sanea(i).VAR03             ,          
                                                         v_bk_sanea(i).VAR04             ,          
                                                         v_bk_sanea(i).VAR05             ,          
                                                         v_bk_sanea(i).CADG_NIT          ,          
                                                         v_bk_sanea(i).CADG_CX_POST      ,          
                                                         v_bk_sanea(i).CADG_CEP_CXP      ,          
                                                         v_bk_sanea(i).CADG_DDD_TEL      ,          
                                                         v_bk_sanea(i).CADG_TEL          ,          
                                                         v_bk_sanea(i).CADG_DDD_FAX      ,          
                                                         v_bk_sanea(i).CADG_FAX          ,          
                                                         v_bk_sanea(i).CADG_CLAS_RI      ,          
                                                         v_bk_sanea(i).MIBGE_COD_MUN     ,          
                                                         v_bk_sanea(i).CADG_DAT_LAUDO    ,          
                                                         v_bk_sanea(i).CADG_IND_NIF      ,          
                                                         v_bk_sanea(i).CADG_DSC_NIF      ,          
                                                         v_bk_sanea(i).IREX_COD          ,          
                                                         v_bk_sanea(i).CADG_IND_OB_CIVIL ,          
                                                         v_bk_sanea(i).SIST_ORIGEM       ,          
                                                         v_bk_sanea(i).USUA_ORIGEM       ,          
                                                         v_bk_sanea(i).DATA_CRIACAO      ,          
                                                         v_bk_sanea(i).ID_ORIGEM         ,          
                                                         v_bk_sanea(i).FTRB_COD          ,          
                                                         v_bk_sanea(i).CADG_INF_ISEN     ,          
                                                         v_bk_sanea(i).CADG_PROVINCIA       -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                         );
				EXCEPTION
				WHEN ex_dml_errors THEN
					BEGIN 
						l_error_count := SQL%BULK_EXCEPTIONS.count;
						FOR i IN 1 .. l_error_count LOOP			
							IF -SQL%BULK_EXCEPTIONS(i).ERROR_CODE != -1 THEN
								v_error_bk    := SUBSTR('Error: ' || i ||  ' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||  ' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),1,500) || ' | ' || SUBSTR(v_error_bk,1,3490);		
							END IF;				 
						END LOOP;
					EXCEPTION
					WHEN OTHERS THEN
						v_error_bk    := NULL;
					END;
					IF NVL(LENGTH(TRIM(v_error_bk)),0) > 0 THEN
						v_error_bk    := SUBSTR('Number of failures: ' || l_error_count,1,500) || ' | ' ||	 SUBSTR(v_error_bk,1,3490);
						prcts_debug(v_error_bk);
					    RAISE_APPLICATION_ERROR (-20343, 'STOP! ' || SUBSTR(v_ds_etapa,1,1000));						
					END IF;
				END;
				:v_qtd_atu_cli := :v_qtd_atu_cli + v_bk_sanea.COUNT - l_error_count;
				${COMMIT};
			END IF;       
			${COMMIT};	
			prcts_debug(' - reg:' || :v_qtd_atu_inf || ' >> ' || :v_qtd_atu_cli || ' >> ' || :v_qtd_processados);
			EXIT WHEN c_sanea%NOTFOUND;	  
		END LOOP;        
		CLOSE c_sanea;   
	END IF;
    ${COMMIT};		
    prc_tempo('Fim - Processados ${COMMIT}:      ' || ' - reg:' || :v_qtd_atu_inf || ' >> ' || :v_qtd_atu_cli || ' >> ' || :v_qtd_processados);
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
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc          = SYSDATE,
       cp.st_processamento     = :v_st_processamento,
       cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_atualizados_nf    = NVL(cp.qt_atualizados_nf,0)   + :v_qtd_processados,
	   cp.qt_atualizados_inf   = NVL(cp.qt_atualizados_inf,0)  + :v_qtd_atu_inf,
	   cp.qt_atualizados_cli  = NVL(cp.qt_atualizados_cli,0)   + :v_qtd_atu_cli
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF                

RETORNO=$?

${WAIT}

exit ${RETORNO}

