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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_COPIA_COMPLVU_CLIFORNEC'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_atu_inf         NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_COPIA_COMPLVU_CLIFORNEC
PROMPT ### Inicio do processo ${0}  ###
PROMPT
DECLARE

	v_action_name VARCHAR2(32) := substr('MAP_2_COPIA_COMPLVU_CLIFORNEC',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);

	CONSTANTE_LIMIT PLS_INTEGER := 50000; 

	l_error_count  NUMBER;    
	ex_dml_errors  EXCEPTION;
	PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
	v_error_bk     VARCHAR2(4000);
	
    CURSOR c_sanea(p_dt IN DATE) 
		IS 
			SELECT /*+ PARALLEL (8,8) */  
				comp.CADG_COD         ,
				comp.CATG_COD         ,
				tmp.DT_PERIODO CADG_DAT_ATUA    ,
				comp.CADG_TIP_ASSIN   ,
				comp.CADG_TIP_UTILIZ  ,
				comp.CADG_GRP_TENSAO  ,
				comp.CADG_TEL_CONTATO ,
				comp.CADG_NUM_CONTA   ,
				comp.CADG_UF_HABILIT  ,
				comp.CADG_TIP_CLI     ,
				comp.CADG_SUB_CONSU   ,
				comp.NUM01            ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				comp.NUM02            ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				comp.NUM03            ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				comp.VAR01            ,	 -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				comp.VAR02            ,	 -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				comp.VAR03            ,	 -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				comp.VAR04            ,	 -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				comp.VAR05               -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
            FROM
				OPENRISOW.COMPLVU_CLIFORNEC        comp,
                GFCADASTRO.TMP_CADASTRO_CLI_COMP   tmp
            WHERE
				comp.rowid = tmp.rowid_comp;
	
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
						INSERT INTO OPENRISOW.COMPLVU_CLIFORNEC (CADG_COD         , 
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
                                                        NUM01            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        NUM02            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        NUM03            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        VAR01            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        VAR02            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        VAR03            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        VAR04            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        VAR05              -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                                        )
                                       VALUES (v_bk_sanea(i).CADG_COD         , 
                                               v_bk_sanea(i).CATG_COD         , 
                                               v_bk_sanea(i).CADG_DAT_ATUA    , 
                                               v_bk_sanea(i).CADG_TIP_ASSIN   , 
                                               v_bk_sanea(i).CADG_TIP_UTILIZ  , 
                                               v_bk_sanea(i).CADG_GRP_TENSAO  , 
                                               v_bk_sanea(i).CADG_TEL_CONTATO , 
                                               v_bk_sanea(i).CADG_NUM_CONTA   , 
                                               v_bk_sanea(i).CADG_UF_HABILIT  , 
                                               v_bk_sanea(i).CADG_TIP_CLI     , 
                                               v_bk_sanea(i).CADG_SUB_CONSU   , 
                                               v_bk_sanea(i).NUM01            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                               v_bk_sanea(i).NUM02            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                               v_bk_sanea(i).NUM03            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                               v_bk_sanea(i).VAR01            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                               v_bk_sanea(i).VAR02            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                               v_bk_sanea(i).VAR03            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                               v_bk_sanea(i).VAR04            , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
                                               v_bk_sanea(i).VAR05              -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
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
				:v_qtd_atu_comp := :v_qtd_atu_comp + v_bk_sanea.COUNT - l_error_count;
				${COMMIT};
			END IF;       
			${COMMIT};	
			prcts_debug(' - reg:' || :v_qtd_atu_inf || ' >> ' || :v_qtd_atu_comp || ' >> ' || :v_qtd_processados);
			EXIT WHEN c_sanea%NOTFOUND;	  
		END LOOP;        
		CLOSE c_sanea;   
	END IF;
    ${COMMIT};		
    prc_tempo('Fim - Processados ${COMMIT}:      ' || ' - reg:' || :v_qtd_atu_inf || ' >> ' || :v_qtd_atu_comp || ' >> ' || :v_qtd_processados);
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
	   cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0)   + :v_qtd_atu_comp
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF                

RETORNO=$?

${WAIT}

exit ${RETORNO}

