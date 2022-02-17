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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_INSERIR_TMP_CADASTRO_DBLINK_TMP_CADASTRO_CLI_COMP.sh'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_atu_cli         NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_INSERIR_TMP_CADASTRO_DBLINK_TMP_CADASTRO_CLI_COMP.sh
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
   v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_INSERIR_TMP_CADASTRO_DBLINK_TMP_CADASTRO_CLI_COMP.sh',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
   
   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   CURSOR c_sanea
       IS
	SELECT /*+ PARALLEL (15) */ DISTINCT A.ROWID_CLI, A.ROWID_COMP 
	FROM GFCADASTRO.TMP_CADASTRO_CLI_COMP@C7 A
	WHERE TRUNC(A.DT_PERIODO)  BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY')
	UNION 	
	SELECT /*+ PARALLEL (15) */ DISTINCT A.ROWID_CLI, A.ROWID_COMP 
	FROM GFCADASTRO.TMP_CADASTRO_CLI_COMP@C7 A
	WHERE ( TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM') = TO_DATE('01/01/2015','DD/MM/YYYY') 
		    AND TRUNC(A.DT_PERIODO) < TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM'))
	;
   TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_sanea t_sanea;
   
   v_cp                  ${TABELA_CONTROLE}%ROWTYPE;
   v_ds_stage            VARCHAR2(4000);
   PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2)
	AS
	BEGIN
		BEGIN
			v_ds_stage := SUBSTR(p_ds_ddo || ' >> ' || v_ds_stage,1,4000);
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_OUTPUT.PUT_LINE(SUBSTR(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || p_ds_ddo ,1,2000));
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_APPLICATION_INFO.set_client_info(SUBSTR(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || v_ds_stage ,1,2000));
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
	EXCEPTION
		WHEN OTHERS THEN
			NULL;
	END;   
	
BEGIN

   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);

   prc_tempo('inicio');  
   
   SELECT *
   INTO   v_cp
   FROM   ${TABELA_CONTROLE} cp
   WHERE  cp.rowid = '${ROWID_CP}';
   
   IF v_cp.DT_LIMITE_INF_NF  = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') THEN
	   prc_tempo('cursor');
	   OPEN c_sanea;
	   LOOP
			FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
			:v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
			prc_tempo('CLI >> ' || :v_qtd_processados || ' >> ' || :v_qtd_atu_cli || ' >> ' || :v_qtd_atu_comp);
			IF v_bk_sanea.COUNT > 0 THEN
				FOR i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST
				LOOP	
					INSERT   INTO  OPENRISOW.TMP_K_C7_CLI_FORNEC_TRANSP (CADG_COD , 
						CADG_DAT_ATUA , 
						CATG_COD , 
						PAIS_COD , 
						UNFE_SIG , 
						CADG_COD_CGCCPF ,
						CADG_TIP , 
						CADG_COD_INSEST ,
						CADG_COD_INSMUN ,
						EQUIPAR_RURAL , 
						CADG_NOM , 
						CADG_NOM_FANTASIA, 
						CADG_END, 
						CADG_END_NUM, 
						CADG_END_COMP, 
						CADG_END_BAIRRO,
						CADG_END_MUNIC,
						CADG_END_CEP, 
						CADG_IND_COLIGADA , 
						CADG_COD_SUFRAMA,
						TP_LOC , 
						LOCA_COD, 
						CADG_CEI, 
						NUM01 , 
						NUM02 , 
						NUM03 , 
						VAR01 , 
						VAR02 , 
						VAR03 , 
						VAR04 , 
						VAR05 , 
						CADG_NIT , 
						CADG_CX_POST , 
						CADG_CEP_CXP , 
						CADG_DDD_TEL , 
						CADG_TEL , 
						CADG_DDD_FAX , 
						CADG_FAX , 
						CADG_CLAS_RI , 
						MIBGE_COD_MUN , 
						CADG_DAT_LAUDO , 
						CADG_IND_NIF , 
						CADG_DSC_NIF , 
						IREX_COD , 
						CADG_IND_OB_CIVIL , 
						SIST_ORIGEM , 
						USUA_ORIGEM , 
						DATA_CRIACAO , 
						ID_ORIGEM , 
						FTRB_COD , 
						CADG_INF_ISEN , 
						CADG_PROVINCIA )
					SELECT CADG_COD , 
							CADG_DAT_ATUA , 
							CATG_COD , 
							PAIS_COD , 
							UNFE_SIG , 
							CADG_COD_CGCCPF ,
							CADG_TIP , 
							CADG_COD_INSEST ,
							CADG_COD_INSMUN ,
							EQUIPAR_RURAL , 
							CADG_NOM , 
							CADG_NOM_FANTASIA, 
							CADG_END, 
							CADG_END_NUM, 
							CADG_END_COMP, 
							CADG_END_BAIRRO,
							CADG_END_MUNIC,
							CADG_END_CEP, 
							CADG_IND_COLIGADA , 
							CADG_COD_SUFRAMA,
							TP_LOC , 
							LOCA_COD, 
							CADG_CEI, 
							NUM01 , 
							NUM02 , 
							NUM03 , 
							VAR01 , 
							VAR02 , 
							VAR03 , 
							VAR04 , 
							VAR05 , 
							CADG_NIT , 
							CADG_CX_POST , 
							CADG_CEP_CXP , 
							CADG_DDD_TEL , 
							CADG_TEL , 
							CADG_DDD_FAX , 
							CADG_FAX , 
							CADG_CLAS_RI , 
							MIBGE_COD_MUN , 
							CADG_DAT_LAUDO , 
							CADG_IND_NIF , 
							CADG_DSC_NIF , 
							IREX_COD , 
							CADG_IND_OB_CIVIL , 
							SIST_ORIGEM , 
							USUA_ORIGEM , 
							DATA_CRIACAO , 
							ID_ORIGEM , 
							FTRB_COD , 
							CADG_INF_ISEN , 
							CADG_PROVINCIA  
					FROM  OPENRISOW.CLI_FORNEC_TRANSP@C7 
					WHERE ROWID = v_bk_sanea(i).ROWID_CLI;  
					:v_qtd_atu_cli       := :v_qtd_atu_cli + 1;
					
					INSERT   INTO  OPENRISOW.TMP_K_C7_COMPLVU_CLIFORNEC (CADG_COD
							, CATG_COD
							, CADG_DAT_ATUA
							, CADG_TIP_ASSIN
							, CADG_TIP_UTILIZ
							, CADG_GRP_TENSAO
							, CADG_TEL_CONTATO
							, CADG_NUM_CONTA
							, CADG_UF_HABILIT
							, CADG_TIP_CLI
							, CADG_SUB_CONSU
							, NUM01
							, NUM02
							, NUM03
							, VAR01
							, VAR02
							, VAR03
							, VAR04
							, VAR05) 
					SELECT  CADG_COD
							, CATG_COD
							, CADG_DAT_ATUA
							, CADG_TIP_ASSIN
							, CADG_TIP_UTILIZ
							, CADG_GRP_TENSAO
							, CADG_TEL_CONTATO
							, CADG_NUM_CONTA
							, CADG_UF_HABILIT
							, CADG_TIP_CLI
							, CADG_SUB_CONSU
							, NUM01
							, NUM02
							, NUM03
							, VAR01
							, VAR02
							, VAR03
							, VAR04
							, VAR05
					FROM OPENRISOW.COMPLVU_CLIFORNEC@C7 WHERE ROWID = v_bk_sanea(i).ROWID_COMP;  
					:v_qtd_atu_comp       := :v_qtd_atu_comp + 1;
					
				END LOOP;	
			END IF;
			${COMMIT};	
			EXIT WHEN c_sanea%NOTFOUND;	  
	   END LOOP;
	   CLOSE c_sanea;
   END IF;
   
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_processados);

   DBMS_APPLICATION_INFO.set_module(null,null);
   DBMS_APPLICATION_INFO.set_client_info (null);

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
      :v_msg_erro := SUBSTR(v_ds_stage || ' >> ' || :v_msg_erro,1,4000);
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
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_nf,0) + :v_qtd_processados,
	   cp.qt_atualizados_cli = NVL(cp.qt_atualizados_cli,0) + :v_qtd_atu_cli,
	   cp.qt_atualizados_comp = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp
 WHERE cp.rowid = '${ROWID_CP}'
   AND cp.NM_PROCESSO = '${PROCESSO}';
COMMIT;


PROMPT Processado

exit :exit_code;

@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

