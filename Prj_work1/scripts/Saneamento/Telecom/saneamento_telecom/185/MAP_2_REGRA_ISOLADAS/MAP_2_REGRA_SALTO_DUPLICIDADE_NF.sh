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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_SALTO_DUPLICIDADE_NF'
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
PROMPT MAP_2_REGRA_SALTO_DUPLICIDADE_NF
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT
BEGIN 
 UPDATE ${TABELA_CONTROLE} cp
   SET cp.ds_msg_erro            = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0) + :v_qtd_processados
 WHERE cp.rowid = '${ROWID_CP}';
 COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE ${TABELA_CONTROLE} cp 
		SET cp.ds_msg_erro               = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
			cp.qt_cad_nao_encontrado     = NVL(cp.qt_cad_nao_encontrado,0) + :v_qtd_processados,
			cp.st_processamento          = 'MAP_2_REGRA_SALTO_DUPLICIDADE_NF'
		WHERE cp.dt_limite_inf_nf        = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') 
		AND UPPER(TRIM(cp.NM_PROCESSO))  = UPPER(TRIM('${PROCESSO}'))
        AND cp.qt_registros_inf > 0
        AND cp.qt_registros_nf  > 0		
		AND ROWNUM < 2;
		COMMIT;
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;          
	END;
END;
/

DECLARE   
 
 	-- Constantes da procedure
	LIMIT_BK_SELECT     CONSTANT NUMBER := 5000;
	
	-- Variaveis
	v_action_name       VARCHAR2(32) := substr('MAP_2_REGRA_SALTO_DUPLICIDADE_NF',1,32);
	v_module_name       VARCHAR2(32) := substr('${PROCESSO}',1,32); 
	v_cp                ${TABELA_CONTROLE}%ROWTYPE;
    v_ds_etapa          VARCHAR2(4000);
    v_nro_dup           PLS_INTEGER := 0;
	v_nro_new           PLS_INTEGER := 0;
	v_inf               openrisow.item_nftl_serv%ROWTYPE;
	v_nf                openrisow.mestre_nftl_serv%ROWTYPE;

	--DEFINI O CURSOR c_sanea_nf----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	CURSOR c_sanea IS
		WITH  	TMP_NF_MENOR AS 
			(
				SELECT nf.* 
				FROM (
					-- MONTH
					SELECT 
						nf.emps_cod, 
						nf.fili_cod, 
						nf.mdoc_cod,
						nf.mnfst_serie, 
						LPAD('0',LENGTH(nf.mnfst_num) ,'0') mnfst_num ,
						nf.mnfst_dtemiss				
						, TO_CHAR(nf.mnfst_dtemiss, 'YYYY-MM')		PERIODO 	
						, TO_NUMBER(0) 		            			NUM_NOTA 
						, ROW_NUMBER() OVER(PARTITION BY nf.emps_cod,nf.fili_cod,nf.mdoc_cod,nf.mnfst_serie, TRUNC(nf.mnfst_dtemiss,'MM') ORDER BY TO_NUMBER(nf.mnfst_num))	rnk
					  FROM OPENRISOW.MESTRE_NFTL_SERV nf
					  WHERE  ${FILTRO} -- nf.emps_cod = 'TBRA' AND nf.fili_cod  IN ('3506','3501') AND nf.mdoc_cod  IN (21,22) AND nf.mnfst_serie = 'UT' -- AND nf.mnfst_dtemiss  >= TO_DATE('01/01/2018','DD/MM/YYYY') AND nf.mnfst_dtemiss  <= TO_DATE('31/01/2018','DD/MM/YYYY')
						AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1')  AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))	
						AND nf.MNFST_DTEMISS < TRUNC(TO_DATE('01/08/2018','DD/MM/YYYY'),'MM') 			  
						AND nf.MNFST_DTEMISS >= TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM')           
						AND nf.MNFST_DTEMISS < ADD_MONTHS(TRUNC(TO_DATE('${DATA_FIM}','DD/MM/YYYY'),'MM'), 1) 	

					) nf         
				WHERE nf.rnk = 1 -- ROWNUM = 1	    
			)    
			, 	TMP_NF_ANT AS --PARTE DO CURSOR c_sanea - NIVEL 1 CONSULTA MESTRE_NFTL_SERV -  PEGA ULTIMA SERIE DO MÊS ANTERIOR
			(
				SELECT nf.* 
				FROM (
					-- MONTH
					SELECT 
						nf.emps_cod, 
						nf.fili_cod, 
						nf.mdoc_cod,
						nf.mnfst_serie, 
						nf.mnfst_num,
						nf.mnfst_dtemiss				
						, 'SEQUENCIAL'						   PERIODO 	
						, TO_NUMBER(nf.mnfst_num) 		       NUM_NOTA 
						, ROW_NUMBER() OVER(PARTITION BY nf.emps_cod,nf.fili_cod,nf.mdoc_cod,nf.mnfst_serie, nf.mnfst_dtemiss ORDER BY TO_NUMBER(nf.mnfst_num) DESC)	rnk
					  FROM OPENRISOW.MESTRE_NFTL_SERV nf
					  WHERE ${FILTRO} --  nf.emps_cod = 'TBRA' AND nf.fili_cod  IN ('3506','3501') AND nf.mdoc_cod  IN (21,22) AND nf.mnfst_serie = 'UT' -- AND nf.mnfst_dtemiss  >= TO_DATE('01/01/2018','DD/MM/YYYY') AND nf.mnfst_dtemiss  <= TO_DATE('31/01/2018','DD/MM/YYYY')
						AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1')  AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))	
						AND nf.mnfst_dtemiss  = (SELECT MAX(MNFST_DTEMISS) FROM OPENRISOW.MESTRE_NFTL_SERV nf1
													WHERE nf1.emps_cod      = nf.emps_cod  
													AND   nf1.fili_cod      = nf.fili_cod   
													AND   nf1.mnfst_serie   = nf.mnfst_serie	
													AND   nf1.mdoc_cod      = nf.mdoc_cod
													AND   nf1.mnfst_dtemiss < (CASE 
																				 WHEN TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM') < TO_DATE('01/08/2018','DD/MM/YYYY') AND  LAST_DAY(TO_DATE('${DATA_FIM}','DD/MM/YYYY'))      > TO_DATE('01/08/2018','DD/MM/YYYY')	 THEN 
																					TO_DATE('01/08/2018','DD/MM/YYYY')																			 
																				 WHEN TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM') >= TO_DATE('01/08/2018','DD/MM/YYYY') THEN
																					TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM')
																				 ELSE NULL
																			   END) 
												)
					) nf
				WHERE nf.rnk = 1 -- ROWNUM = 1	
			)
			, 	TMP_NF AS --PARTE DO CURSOR c_sanea - NIVEL 1 CONSULTA MESTRE_NFTL_SERV - UNE COM A SERIE DO MES ATUAL
			(
				SELECT 
					nf.emps_cod, 
					nf.fili_cod, 
					nf.mdoc_cod,
					nf.mnfst_serie, 
					nf.mnfst_num,
					nf.mnfst_dtemiss				
					, (CASE 
							WHEN nf.mnfst_dtemiss >= TO_DATE('01/08/2018','DD/MM/YYYY') THEN 'SEQUENCIAL'
							ELSE TO_CHAR(nf.mnfst_dtemiss, 'YYYY-MM')
					   END) PERIODO 	
					, TO_NUMBER(nf.mnfst_num) 		       NUM_NOTA 
					, 1 AS rnk
				FROM openrisow.mestre_nftl_serv nf 
				WHERE ${FILTRO} --  nf.emps_cod = 'TBRA' AND nf.fili_cod  IN ('3506','3501') AND nf.mdoc_cod  IN (21,22) AND nf.mnfst_serie = 'UT' -- AND nf.mnfst_dtemiss  >= TO_DATE('01/01/2018','DD/MM/YYYY') AND nf.mnfst_dtemiss  <= TO_DATE('31/01/2018','DD/MM/YYYY')
					AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1') AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))	
					AND nf.MNFST_DTEMISS >= TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM') 
					AND nf.MNFST_DTEMISS < ADD_MONTHS(TRUNC(TO_DATE('${DATA_FIM}','DD/MM/YYYY'),'MM'), 1) 				
			
		)
		,	TMP_REL_NF AS 
		(
			SELECT 
				LEAD(NUM_NOTA) OVER(PARTITION BY emps_cod, fili_cod, mdoc_cod , mnfst_serie, PERIODO ORDER BY NUM_NOTA) PROX_NUM_NOTA
				, TMP.*
			FROM (
				  SELECT * FROM TMP_NF_MENOR
				  UNION ALL
				  (
					SELECT * FROM TMP_NF_ANT
					UNION ALL
					SELECT * FROM TMP_NF
				  )           
			) TMP
		)
		,	TMP_REL AS 
		(
			--PARTE DO CURSOR c_sanea - NIVEL 3 CONSULTA TMP_REL_NF - LEVANTA O QUE ESTA ERRADO BASEADO NA SEQUENCIA LEVANTADA A CIMA 
			SELECT 
			nf.*,
			(CASE 
				WHEN nf.PROX_NUM_NOTA = nf.NUM_NOTA     THEN 'D' -- NF duplicada
				WHEN nf.PROX_NUM_NOTA - nf.NUM_NOTA > 1 THEN 'S' -- Salto de sequencia
				ELSE NULL
			END) TIPO_ERRO,  
			(nf.NUM_NOTA      + 1) INICIO_SALTO,
			(nf.PROX_NUM_NOTA - 1) FIM_SALTO
			FROM TMP_REL_NF nf
			WHERE 	(
						(nf.PROX_NUM_NOTA - nf.NUM_NOTA > 1) 
						OR 
						(nf.PROX_NUM_NOTA = nf.NUM_NOTA)
					)
		)
		SELECT 
		   data_nf.*,
		   (CASE 
				WHEN data_nf.PERIODO  = 'SEQUENCIAL' THEN 
					CASE
						WHEN TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM') > TO_DATE('01/08/2018','DD/MM/YYYY') THEN TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM')
						ELSE TO_DATE('01/08/2018','DD/MM/YYYY')
					END
				ELSE TRUNC(data_nf.DAT_NOTA,'MM')
		   END) PERIDO_INICIAL,
		   (CASE 
				WHEN data_nf.PERIODO  = 'SEQUENCIAL' THEN LAST_DAY(TRUNC(TO_DATE('${DATA_FIM}','DD/MM/YYYY'),'MM'))
				ELSE LAST_DAY(TRUNC(data_nf.DAT_NOTA,'MM'))
		   END) PERIDO_FINAL
		FROM   
        (
			SELECT 
				   tmp.TIPO_ERRO,
				   nf.rowid     		AS rowid_nf,
				   nf.mnfst_num 		AS NUM_NOTA,
				   nf.mnfst_dtemiss 	AS DAT_NOTA,
				   tmp.INICIO_SALTO,
				   tmp.FIM_SALTO,
				   tmp.PERIODO
			FROM openrisow.mestre_nftl_serv nf, 
						 TMP_REL tmp 
			WHERE   tmp.PERIODO       = 'SEQUENCIAL' 
				AND nf.emps_cod       = tmp.emps_cod
				AND nf.fili_cod       = tmp.fili_cod	
				AND nf.mdoc_cod       = tmp.mdoc_cod
				AND nf.mnfst_serie    = tmp.mnfst_serie
				AND TO_NUMBER(nf.mnfst_num)    = tmp.PROX_NUM_NOTA
				AND nf.mnfst_dtemiss >= (CASE
											WHEN TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM') > TO_DATE('01/08/2018','DD/MM/YYYY') THEN TRUNC(TO_DATE('${DATA_INICIO}','DD/MM/YYYY'),'MM')
											ELSE TO_DATE('01/08/2018','DD/MM/YYYY')
										END)
				AND nf.mnfst_dtemiss  <= LAST_DAY(TRUNC(TO_DATE('${DATA_FIM}','DD/MM/YYYY'),'MM'))
			UNION ALL															
			SELECT 
				tmp.TIPO_ERRO,
				nf.rowid     			AS rowid_nf,
				nf.mnfst_num 			AS NUM_NOTA,
				nf.mnfst_dtemiss 		AS DAT_NOTA,
				tmp.INICIO_SALTO,
				tmp.FIM_SALTO,
				tmp.PERIODO
			FROM openrisow.mestre_nftl_serv nf, 
						 TMP_REL tmp 
			WHERE  tmp.PERIODO        != 'SEQUENCIAL' 
				AND nf.emps_cod        = tmp.emps_cod
				AND nf.fili_cod        = tmp.fili_cod	
				AND nf.mdoc_cod        = tmp.mdoc_cod
				AND nf.mnfst_serie     = tmp.mnfst_serie
				AND TO_NUMBER(nf.mnfst_num)    = tmp.PROX_NUM_NOTA
				AND nf.mnfst_dtemiss  >=  TRUNC(tmp.mnfst_dtemiss,'MM') 
				AND nf.mnfst_dtemiss  <=  LAST_DAY(TRUNC(tmp.mnfst_dtemiss,'MM'))
		) data_nf
		-- ORDER BY 1 DESC
		;
	TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;   
	v_bk_sanea 	t_sanea;
    v_sanea 	c_sanea%ROWTYPE;
	
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
			UPDATE ${TABELA_CONTROLE} cp   
			SET cp.ds_msg_erro          = substr(substr(nvl(v_ds_etapa,' '),1,3900) || ' >> ' || cp.ds_msg_erro ,1,4000)
			WHERE cp.rowid = '${ROWID_CP}';	
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
		
		COMMIT;
		
	END;	
	
	PROCEDURE prcts_stop(p_ds_ddo IN VARCHAR2 := NULL) AS 		
	BEGIN
		BEGIN
			v_ds_etapa := substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' : ' ||  p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		EXCEPTION
		 WHEN OTHERS THEN
		   NULL;
		END;
		prc_debug;
		RAISE_APPLICATION_ERROR (-20343, 'STOP! ' || SUBSTR(v_ds_etapa,1,1000));
	END;	   
	

BEGIN

	-----------------------------------------------------------------------------
	--> Nomeando o processo
	-----------------------------------------------------------------------------	
	DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
	DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);

	IF NVL(LENGTH(TRIM('${ROWID_CP}')),0) > 0 THEN
	   BEGIN
		 SELECT *
		 INTO   v_cp
		 FROM   ${TABELA_CONTROLE} cp
		 WHERE  cp.rowid = '${ROWID_CP}';   
	   EXCEPTION
			WHEN OTHERS THEN
				v_cp.dt_limite_inf_nf := TO_DATE('${DATA_INICIO}','DD/MM/YYYY');
	   END;
    ELSE
	   v_cp.dt_limite_inf_nf := TO_DATE('${DATA_INICIO}','DD/MM/YYYY');	
    END IF;
     
	v_action_name := substr(to_char(v_cp.dt_limite_inf_nf,'DD/MM/YYYY') || ' >> ' || v_action_name,1,32);
	DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);   

	IF v_cp.dt_limite_inf_nf = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') THEN
	
	
		OPEN c_sanea;
		LOOP -- PERCORRE O CURSOR c_sanea ONDE CADA LINHA CONTEM UM INTERVALO DE NOTAS A SER CORRIGIDO 
			FETCH c_sanea BULK COLLECT
		    INTO v_bk_sanea LIMIT LIMIT_BK_SELECT;
			
			:v_qtd_processados := :v_qtd_processados + v_bk_sanea.COUNT;
			
			IF v_bk_sanea.COUNT > 0 THEN
			
				--CASO DE FATO EXISTA SEQUENCIA INVALIDAS
				FOR i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST
				LOOP --PERCORRE SEQUENCIAS 
					
					
					v_sanea := v_bk_sanea(i);
					
					IF v_sanea.TIPO_ERRO = 'S' THEN	-- Salto de sequencia				
				
						SELECT nf.*
						INTO   v_nf
						FROM   openrisow.mestre_nftl_serv nf 
						WHERE  nf.rowid   =  v_sanea.rowid_nf;
						
						FOR c_mnfst_num in v_sanea.INICIO_SALTO..v_sanea.FIM_SALTO LOOP
							
							v_nf.mnfst_num  :=  LPAD(TO_CHAR(c_mnfst_num), 9, '0');
								
							v_nf.var05 		    := 'REGRA_SALTO_SEQUENCIA';
							v_nf.mnfst_ind_canc := 'S';
								
							--insere nota novas cancelados
							INSERT /*+ ignore_row_on_dupkey_index(MESTRE_NFTL_SERV,MESTRE_NFTL_SERVP1) */  
							INTO   openrisow.mestre_nftl_serv
							VALUES v_nf;
							:v_qtd_atu_nf := :v_qtd_atu_nf + SQL%ROWCOUNT;	
						
						END LOOP;
						
						DELETE /*+ parallel(15) */ 
						FROM  openrisow.item_nftl_serv inf
						WHERE inf.emps_cod                           =  v_nf.emps_cod
						AND   inf.fili_cod                           =  v_nf.fili_cod
						AND   inf.infst_serie                        =  v_nf.mnfst_serie
						AND   TO_NUMBER(inf.infst_num)              BETWEEN  v_sanea.INICIO_SALTO   AND  v_sanea.FIM_SALTO
						AND   inf.infst_dtemiss                     BETWEEN  v_sanea.PERIDO_INICIAL AND  v_sanea.PERIDO_FINAL;  
												
						FOR c_inf in (SELECT /*+ parallel(15) */  inf.* 
						              FROM   openrisow.item_nftl_serv inf
									  WHERE  inf.emps_cod       =  v_nf.emps_cod
									  AND    inf.fili_cod       =  v_nf.fili_cod
									  AND    inf.infst_serie    =  v_nf.mnfst_serie
									  AND    inf.infst_dtemiss  =  v_nf.mnfst_dtemiss
									  AND    inf.infst_num      =  v_sanea.NUM_NOTA) LOOP
							
							v_inf 				 := c_inf;
							v_inf.var05 		 := v_nf.var05;
							v_inf.infst_ind_canc := v_nf.mnfst_ind_canc;
							
							FOR c_infst_num in v_sanea.INICIO_SALTO..v_sanea.FIM_SALTO LOOP
							
								v_inf.infst_num  :=  LPAD(TO_CHAR(c_infst_num), 9, '0');
								
								INSERT /*+ ignore_row_on_dupkey_index(ITEM_NFTL_SERV,ITEM_NFTL_SERVP1) */ 
								INTO   openrisow.item_nftl_serv
								VALUES v_inf;
								:v_qtd_atu_inf := :v_qtd_atu_inf + SQL%ROWCOUNT;	
								
							END LOOP;
						
						END LOOP;
						
						v_nro_new := v_nro_new + (TO_NUMBER(v_sanea.FIM_SALTO)-TO_NUMBER(v_sanea.INICIO_SALTO))+1;
						
						${COMMIT};
						
					ELSE
					
						v_nro_dup := v_nro_dup + 1;
					
					END IF;								
				
				END LOOP;
			
				${COMMIT};
			
			END IF;			
			
			EXIT WHEN c_sanea%NOTFOUND;
			  
		END LOOP;

		CLOSE c_sanea; 		
	
	
	END IF;

	:v_qtd_atu_cli  := v_nro_dup;
	:v_qtd_atu_comp := v_nro_new;
	
	${COMMIT};
	prc_tempo('FIM');
	prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> NF : ' || :v_qtd_atu_nf || ' >> INF : ' || :v_qtd_atu_inf  || ' >> NF CRIADO : ' || v_nro_new || ' >> NF DUPLICADO : ' || v_nro_dup );
	
	IF v_nro_dup > 0 THEN 	
		prcts_stop('PARADA OBRIGATORIA - NOTA FISCAL DUPLICADA. ' || v_nro_dup || ' . NF CRIADOS : ' || v_nro_new);		
	END IF;
	
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
BEGIN
	UPDATE ${TABELA_CONTROLE} cp
	   SET cp.dt_fim_proc          = SYSDATE,
		   cp.st_processamento     = :v_st_processamento,
		   cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,1000) || ' >> ' || cp.ds_msg_erro ,1,4000),
		   cp.qt_atualizados_nf    = NVL(cp.qt_atualizados_nf,0)   + :v_qtd_atu_nf,
		   cp.qt_atualizados_inf   = NVL(cp.qt_atualizados_inf,0)  + :v_qtd_atu_inf,
		   cp.qt_atualizados_cli   = NVL(cp.qt_atualizados_cli,0)  + :v_qtd_atu_cli,
		   cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp--,
	 WHERE cp.rowid = '${ROWID_CP}';
	COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE ${TABELA_CONTROLE}  cp
		SET cp.ds_msg_erro              = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
			cp.qt_atualizados_nf        = NVL(cp.qt_atualizados_nf,0)   + :v_qtd_atu_nf,
		    cp.qt_atualizados_inf       = NVL(cp.qt_atualizados_inf,0)  + :v_qtd_atu_inf,
		    cp.qt_atualizados_cli       = NVL(cp.qt_atualizados_cli,0)  + :v_qtd_atu_cli,
		    cp.qt_atualizados_comp      = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp,
			cp.st_processamento         = DECODE(UPPER(TRIM(:v_st_processamento)),'ERRO',:v_st_processamento,'Aguardando')
		WHERE cp.dt_limite_inf_nf       = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') 
		AND UPPER(TRIM(cp.NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'))
        AND cp.qt_registros_inf         > 0
        AND cp.qt_registros_nf          > 0		
		AND ROWNUM                      < 2;
		COMMIT;
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;          
	END;
END;
/
PROMPT Processado 

exit :exit_code;

@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

