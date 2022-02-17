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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_32_REALOCACAO_CFOP_0000_ORIGINAL'
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
PROMPT MAP_2_REGRA_32_REALOCACAO_CFOP_0000_ORIGINAL
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
			UPPER(TRANSLATE(nf.mnfst_serie, 'x ', 'x')) serie,
			inf.rowid             rowid_inf,  
            MAX(inf.cfop) OVER ( PARTITION BY nf.rowid ) cfop_max,			
            to_NUMBER(0) update_reg,
			inf.*
	FROM    openrisow.item_nftl_serv      PARTITION (${PARTICAO_INF}) inf,  
			openrisow.mestre_nftl_serv    PARTITION (${PARTICAO_NF})  nf   
	WHERE   ${FILTRO}
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
		AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
		  AND inf.emps_cod                                               = nf.emps_cod
		  AND inf.fili_cod                                               = nf.fili_cod
		  AND inf.infst_serie                                            = nf.mnfst_serie
		  AND inf.infst_num                                              = nf.mnfst_num
		  AND inf.infst_dtemiss                                          = nf.mnfst_dtemiss
		  AND inf.mdoc_cod                                               = nf.mdoc_cod	  
    ORDER BY rowid_nf,   last_ref_nf ;
    v_inf                 c_inf%ROWTYPE;
    v_pref_servfz         VARCHAR2(10);
    v_pref_servfzp        VARCHAR2(10);
    v_pref_servfzn        VARCHAR2(10);	
	v_serv_cod            openrisow.item_nftl_serv.serv_cod%type; 
	v_fl_exists 		  PLS_INTEGER := 0;
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
   
    -- Funcoes Gerais
	FUNCTION fccts_retira_caracter(p_ds_ddo IN VARCHAR2, p_ds_serie IN VARCHAR2) RETURN VARCHAR2 IS
		v_ds_serie_inf    VARCHAR2(500);
		v_ds_serv_cod_inf VARCHAR2(1000); 
		v_nr_length       PLS_INTEGER;
		v_nr_length_aux   PLS_INTEGER;
	BEGIN		
		v_ds_serie_inf    := TRIM(UPPER(TRANSLATE(p_ds_serie,'| ','|')));
		v_nr_length       := NVL(LENGTH(v_ds_serie_inf),0);
		v_ds_serv_cod_inf := TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p_ds_ddo,v_ds_serie_inf||'ZP',''),v_ds_serie_inf||'ZN',''),v_ds_serie_inf||'C08',''),v_ds_serie_inf||'C09',''),v_ds_serie_inf||'Z',''),v_ds_serie_inf||'C','')); 
		FOR I IN 65..75 LOOP
			v_ds_serv_cod_inf := TRIM(REPLACE(v_ds_serv_cod_inf,v_ds_serie_inf||CHR(I),'')); 
		END LOOP;
		v_nr_length_aux       := NVL(LENGTH(v_ds_serv_cod_inf),0);
		IF v_nr_length_aux > v_nr_length THEN
		  IF SUBSTR(v_ds_serv_cod_inf,1,v_nr_length) = v_ds_serie_inf THEN
			v_ds_serv_cod_inf := TRIM(SUBSTR(v_ds_serv_cod_inf,v_nr_length+1,v_nr_length_aux));
		  END IF;
		END IF;
		RETURN v_ds_serv_cod_inf;
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
	end if;	
	
	IF (v_inf.CFOP = '0000' AND v_inf.rowid_inf IS NOT NULL )
	THEN
		v_fl_exists      := 0;
		v_serv_cod       := TRIM(SUBSTR(fccts_retira_caracter(p_ds_ddo => v_inf.serv_cod, p_ds_serie => v_inf.serie),1,60));  
		BEGIN
			SELECT /*+ first_rows(1)*/ NVL(COUNT(1),0) INTO v_fl_exists FROM DUAL
			WHERE EXISTS (
				SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD))= TRIM(UPPER(v_inf.serv_cod))
				UNION ALL
				SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD_ORIGINAL))= TRIM(UPPER(v_inf.serv_cod))
				UNION ALL
				SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD))= TRIM(UPPER(v_serv_cod))
				UNION ALL
				SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD_ORIGINAL))= TRIM(UPPER(v_serv_cod))
			);
		EXCEPTION
		WHEN OTHERS THEN
			v_fl_exists := 0;
		END;	

		IF v_fl_exists > 0 THEN
			v_inf.update_reg           			    := 1;
			v_inf.var05             				:= substr('prcts_regra_32u:' || v_inf.CFOP || '|' || v_inf.SERV_COD  || '>>' ||v_inf.var05,1,150);
			-- Altera o registro original
			v_inf.CFOP                              := (CASE
															WHEN UPPER(TRIM(NVL(v_inf.cfop_max,'0000'))) = '0000' THEN
															   CASE 
																   WHEN  NVL(LENGTH(NVL(TRIM(v_inf.CGC_CPF),'9999')),0) > 11 THEN -- PJ 
																	-- Se cliente PJ CFOP = '5303'
																	'5303'
																   ELSE
																	-- Se cliente PF CFOP = '5307'
																	'5307'
															   END
															ELSE
															   UPPER(TRIM(NVL(v_inf.cfop_max,'0000')))
														END);
			v_inf.SERV_COD                          := v_serv_cod;
		END IF;

		IF v_inf.update_reg = 1 THEN

		  UPDATE openrisow.item_nftl_serv inf
			SET inf.var05              = substr(v_inf.VAR05,1,150) 	 			
			  , inf.CFOP               = v_inf.CFOP
			  , inf.SERV_COD           = v_inf.SERV_COD 
		  WHERE rowid = v_inf.rowid_inf;
		  v_inf.update_reg   := 0;
		  :v_qtd_atu_inf     := :v_qtd_atu_inf + 1;
		  
		END IF;
	
	END IF;
	
	IF v_inf.last_ref_nf  = 'S' THEN 	
		${COMMIT};
	END IF;
			
   END LOOP;
   CLOSE c_inf;
   
   ${COMMIT};
   prc_tempo('FIM');
   prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> INF : ' || :v_qtd_atu_inf );

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

