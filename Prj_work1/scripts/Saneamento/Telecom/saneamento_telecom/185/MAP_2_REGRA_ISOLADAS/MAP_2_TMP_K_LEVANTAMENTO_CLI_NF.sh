#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
FILTRO_SCRIPT="${FILTRO}"
#tratamento dos parametros
FILTRO_SCRIPT="${FILTRO_SCRIPT^^}" 
FILTRO_SCRIPT="${FILTRO_SCRIPT##*( )}" 
FILTRO_SCRIPT="${FILTRO_SCRIPT%%*( )}"
case ${FILTRO_SCRIPT} in
   "")
		FILTRO_SCRIPT="f.emps_cod = 'TBRA'"	
	;;
    *)
        FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND f.emps_cod = 'TBRA'" 
	;;
esac
sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_TMP_K_LEVANTAMENTO_CLI_NF'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_TMP_K_LEVANTAMENTO_CLI_NF
PROMPT ### Inicio do processo ${0}  ###
PROMPT
DECLARE
-- CREATE TABLE openrisow.tmp_k_levantamento_cli_nf (
--     cadg_cod              VARCHAR2(16) NOT NULL,
--     catg_cod              CHAR(2) NOT NULL,
--     unfe_sig              CHAR(2) NOT NULL,
--     cadg_cod_insest       VARCHAR2(14) NOT NULL,
--     cadg_end_cep          CHAR(8) NOT NULL,
--     cadg_cod_cgccpf       VARCHAR2(16) NOT NULL,
--     tip_nota              VARCHAR2(500) NOT NULL ,
--     err_cadg_cod_insest   VARCHAR2(500)  DEFAULT '0' NOT NULL,
--     CONSTRAINT tmp_k_levantamento_cli_nfp1 PRIMARY KEY ( cadg_cod,
--                                                          catg_cod,
--                                                          unfe_sig,
--                                                          cadg_cod_insest,
--                                                          cadg_end_cep,
--                                                          cadg_cod_cgccpf)
-- )
-- TABLESPACE openris_data;

	v_action_name VARCHAR2(32) := substr('MAP_2_TMP_K_LEVANTAMENTO_CLI_NF',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);

	CONSTANTE_LIMIT PLS_INTEGER := 500000; 

	l_error_count  NUMBER;    
	ex_dml_errors  EXCEPTION;
	PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
	v_error_bk     VARCHAR2(4000);
	
    CURSOR c_sanea -- (p_dt IN DATE) 
		IS 
		WITH tmp_cli AS (
			SELECT /*+ PARALLEL(16) */
				cli1.cadg_cod,
				cli1.catg_cod,
				cli1.unfe_sig,
				cli1.cadg_cod_insest,
				cli1.cadg_end_cep,
				cli1.cadg_cod_cgccpf,
				'SAIDA' AS tip_nota
			  -- ,  gfcadastro.fnc_valida_inscricao_estadual(p_cc_estado => cli1.unfe_sig, p_cc_inscricao_estadual => cli1.cadg_cod_insest) AS  err_cadg_cod_insest
			FROM
				openrisow.complvu_clifornec comp,
				(
					SELECT
						cli.cadg_cod,
						cli.catg_cod,
						cli.unfe_sig,
						cli.cadg_cod_insest,
						cli.cadg_end_cep,
						cli.cadg_cod_cgccpf,
						cli.cadg_dat_atua,
						ROW_NUMBER() OVER(
							PARTITION BY nf.rowid
							ORDER BY
								cli.cadg_dat_atua DESC
						) rnk1
					FROM
						openrisow.cli_fornec_transp   cli,
						openrisow.mestre_nftl_serv    nf,
						openrisow.filial              f
					WHERE ${FILTRO_SCRIPT} 
						-- f.fili_cod_insest = '029494400'
						AND nf.emps_cod = f.emps_cod
						AND nf.fili_cod = f.fili_cod
						AND nf.mnfst_dtemiss >= TO_DATE('01/07/2015', 'dd/mm/yyyy')
						AND cli.cadg_cod = nf.cadg_cod
						AND cli.catg_cod = nf.catg_cod
						AND cli.cadg_dat_atua <= nf.mnfst_dtemiss
				) cli1
			WHERE
				cli1.rnk1                              = 1
				AND comp.cadg_cod                      = cli1.cadg_cod
				AND comp.catg_cod                      = cli1.catg_cod
				AND comp.cadg_dat_atua                 = cli1.cadg_dat_atua
				AND NVL(trim(comp.cadg_tip_assin),'1') = '1'
			GROUP BY
				cli1.cadg_cod,
				cli1.catg_cod,
				cli1.unfe_sig,
				cli1.cadg_cod_insest,
				cli1.cadg_end_cep,
				cli1.cadg_cod_cgccpf
		)
		SELECT /*+ PARALLEL(16) */
			nvl(tmp.cadg_cod,'NA') as cadg_cod,
			nvl(tmp.catg_cod,'NA') as catg_cod,
			nvl(tmp.unfe_sig,'NA') as unfe_sig,
			nvl(tmp.cadg_cod_insest,'NA') as cadg_cod_insest,
			nvl(tmp.cadg_end_cep,'NA') as cadg_end_cep,
			nvl(tmp.cadg_cod_cgccpf,'NA') as cadg_cod_cgccpf,
			nvl(tmp.tip_nota,'NA') as tip_nota
			-- tmp.err_cadg_cod_insest
			, nvl(gfcadastro.fnc_valida_inscricao_estadual(p_cc_estado => tmp.unfe_sig, p_cc_inscricao_estadual => tmp.cadg_cod_insest),'1') AS err_cadg_cod_insest
		FROM
			tmp_cli tmp
		UNION ALL
		SELECT /*+ PARALLEL(16) */
			nvl(tmp3.cadg_cod,'NA') as cadg_cod,
			nvl(tmp3.catg_cod,'NA') as catg_cod,
			nvl(tmp3.unfe_sig,'NA') as unfe_sig,
			nvl(tmp3.cadg_cod_insest,'NA') as cadg_cod_insest,
			nvl(tmp3.cadg_end_cep,'NA') as cadg_end_cep,
			nvl(tmp3.cadg_cod_cgccpf,'NA') as cadg_cod_cgccpf,
			'ENTRADA' AS tip_nota
			, nvl(gfcadastro.fnc_valida_inscricao_estadual(p_cc_estado => tmp3.unfe_sig, p_cc_inscricao_estadual => tmp3.cadg_cod_insest),'1') AS err_cadg_cod_insest
		FROM
			(
				SELECT
					cli2.cadg_cod,
					cli2.catg_cod,
					cli2.unfe_sig,
					cli2.cadg_cod_insest,
					cli2.cadg_end_cep,
					cli2.cadg_cod_cgccpf
				FROM
					(
						SELECT
							cli.cadg_cod,
							cli.catg_cod,
							cli.unfe_sig,
							cli.cadg_cod_insest,
							cli.cadg_end_cep,
							cli.cadg_cod_cgccpf,
							ROW_NUMBER() OVER(
								PARTITION BY nf.rowid
								ORDER BY
									cli.cadg_dat_atua DESC
							) rnk1
						FROM
							openrisow.cli_fornec_transp   cli,
							openrisow.mestre_nfen_merc    nf,
							openrisow.filial              f
						WHERE ${FILTRO_SCRIPT} 
							-- f.fili_cod_insest = '029494400'
							AND nf.emps_cod = f.emps_cod
							AND nf.fili_cod = f.fili_cod                    
							AND nf.mnfem_dtentr >= TO_DATE('01/01/2015', 'dd/mm/yyyy')
							AND ( ( nf.mdoc_cod IN (
								'6',
								'06',
								'006',
								'21',
								'021',
								'22',
								'022',
								'28',
								'028',
								'29',
								'029'
							) )
								  OR ( nf.mdoc_cod IN (
								'1',
								'01',
								'001',
								'55',
								'055'
							)
									   AND EXISTS (
								SELECT /*+ FIRST_ROWS(1) */
									1
								FROM
									openrisow.item_nfem_merc inf
								WHERE
									inf.emps_cod = nf.emps_cod
									AND inf.fili_cod = nf.fili_cod
									AND inf.infem_serie = nf.mnfem_serie
									AND inf.infem_num = nf.mnfem_num
									AND inf.infem_dtemis = nf.mnfem_dtemis
									AND inf.catg_cod = nf.catg_cod
									AND inf.cadg_cod = nf.cadg_cod
									AND ( ( inf.cfop_cod >= '1250'
											AND inf.cfop_cod <= '1257' )
										  OR ( inf.cfop_cod >= '2250'
											   AND inf.cfop_cod <= '2257' ) )
							) ) )
							AND cli.cadg_cod = nf.cadg_cod
							AND cli.catg_cod = nf.catg_cod
							AND cli.cadg_dat_atua <= nf.mnfem_dtentr
					) cli2
				WHERE
					cli2.rnk1 = 1
				GROUP BY
					cli2.cadg_cod,
					cli2.catg_cod,
					cli2.unfe_sig,
					cli2.cadg_cod_insest,
					cli2.cadg_end_cep,
					cli2.cadg_cod_cgccpf
			) tmp3
		WHERE
			NOT EXISTS (
				SELECT /*+ FIRST_ROWS(1) */
					1
				FROM
					tmp_cli tmp2
				WHERE
					tmp2.cadg_cod = tmp3.cadg_cod
					AND tmp2.catg_cod = tmp3.catg_cod
					AND tmp2.unfe_sig = tmp3.unfe_sig
					AND tmp2.cadg_cod_insest = tmp3.cadg_cod_insest
					AND tmp2.cadg_end_cep = tmp3.cadg_end_cep
					AND tmp2.cadg_cod_cgccpf = tmp3.cadg_cod_cgccpf
			);
	
		
    TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
    v_bk_sanea t_sanea;
    v_sanea    c_sanea%ROWTYPE;

	v_dt_aux              DATE; 
    v_ds_etapa            VARCHAR2(4000);
    PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
    BEGIN
		BEGIN
			v_ds_etapa := substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
			DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_APPLICATION_INFO.set_client_info(substr(v_ds_etapa ,1,62));				 	
		EXCEPTION
		 WHEN OTHERS THEN
		   NULL;
		END;
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
   
    IF TRUNC(v_dt_aux) = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') THEN 
	-- FOR C IN (SELECT TRUNC(DT_EXECUCAO,'YYYY') DT_EXECUCAO FROM (select  (TO_DATE('${DATA_INICIO}','DD/MM/YYYY')-1) + rownum DT_EXECUCAO  from DUAL connect by level  < 20000) WHERE DT_EXECUCAO < TO_DATE('01/01/2021','DD/MM/YYYY') GROUP BY TRUNC(DT_EXECUCAO,'YYYY') ORDER BY DT_EXECUCAO)
	-- LOOP 	
		:v_qtd_atu_comp := 0;
		prc_tempo('SANEA: ');-- || TO_CHAR(C.DT_EXECUCAO,'DD/MM/YYYY'));
		OPEN c_sanea;--(p_dt=>C.DT_EXECUCAO);
		LOOP
			FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
			:v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
			-- TO_CHAR(C.DT_EXECUCAO,'DD/MM/YYYY') || 
			prcts_debug('>> BEFORE : ' || :v_qtd_atu_comp || ' >> ' || :v_qtd_processados);
			IF v_bk_sanea.COUNT > 0 THEN
				BEGIN
					l_error_count :=0;
					v_error_bk := NULL;
					FORALL i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST SAVE EXCEPTIONS
						INSERT INTO openrisow.tmp_k_levantamento_cli_nf(CADG_COD,CATG_COD,UNFE_SIG,
																		CADG_COD_INSEST,CADG_END_CEP,CADG_COD_CGCCPF,
																		TIP_NOTA,ERR_CADG_COD_INSEST)  
						VALUES (v_bk_sanea(i).CADG_COD,v_bk_sanea(i).CATG_COD,v_bk_sanea(i).UNFE_SIG,
								v_bk_sanea(i).CADG_COD_INSEST,v_bk_sanea(i).CADG_END_CEP,v_bk_sanea(i).CADG_COD_CGCCPF,
								v_bk_sanea(i).TIP_NOTA,v_bk_sanea(i).ERR_CADG_COD_INSEST);
	
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
					    RAISE_APPLICATION_ERROR (-20343, 'STOP >> ' || SUBSTR(v_ds_etapa,1,1000));						
					END IF;
				END;
				:v_qtd_atu_comp := :v_qtd_atu_comp + v_bk_sanea.COUNT - l_error_count;
				${COMMIT};
			END IF;       
			${COMMIT};	
			-- TO_CHAR(C.DT_EXECUCAO,'DD/MM/YYYY') || 
			prcts_debug(' >> AFTER : ' || :v_qtd_atu_comp || ' >> ' || :v_qtd_processados);
			EXIT WHEN c_sanea%NOTFOUND;	  
		END LOOP;        
		CLOSE c_sanea;   
	-- END LOOP;
	END IF;
    ${COMMIT};		
    prc_tempo('Fim - ${COMMIT}:      ' || ' >> ' || :v_qtd_processados);
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
       cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0)   + :v_qtd_atu_comp
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF                

RETORNO=$?

${WAIT}

exit ${RETORNO}

