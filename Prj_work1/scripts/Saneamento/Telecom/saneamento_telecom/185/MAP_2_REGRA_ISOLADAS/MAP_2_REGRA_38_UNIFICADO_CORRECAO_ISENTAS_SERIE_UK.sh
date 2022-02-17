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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_38_UNIFICADO_CORRECAO_ISENTAS_SERIE_UK'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_ins_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_reg_paralizacao NUMBER         = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_38_UNIFICADO_CORRECAO_ISENTAS_SERIE_UK
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT
DECLARE
   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
  
   v_mnfst_dtemiss openrisow.mestre_nftl_serv.mnfst_dtemiss%TYPE;
   
   CURSOR c_sanea--(p_mnfst_dtemiss openrisow.mestre_nftl_serv.mnfst_dtemiss%TYPE)
    IS	
	SELECT  /*+ parallel(15) */    
		UPPER(TRIM(TRANSLATE(nf.mnfst_serie,'x ','x'))) serie,     
		nf.rowid    AS rowid_nf,  
		inf.rowid AS rowid_inf,  
        inf.*   
	FROM ${TABELA_CFOP_NEGATIVO}    pt,
	     openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf,     
	     openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) inf   
	WHERE ${FILTRO} 
	    -- AND TRUNC(pt.mnfst_dtemiss,'MM')              = TRUNC(p_mnfst_dtemiss,'MM')
		AND TRUNC(nf.mnfst_dtemiss,'MM')              = TRUNC(pt.mnfst_dtemiss,'MM')
		AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) = pt.mnfst_serie
	    AND nf.emps_cod                               = pt.emps_cod
		AND nf.fili_cod                               = pt.fili_cod
		AND nf.mdoc_cod                               = pt.mdoc_cod
		AND TO_NUMBER(nf.mnfst_num)                   = pt.mnfst_num	
		AND (NVL(nf.mnfst_val_basicms,0)             != NVL(pt.mnfst_val_basicms,0)
            OR NVL(nf.mnfst_val_tot,0)               != NVL(pt.mnfst_val_tot,0)
            OR NVL(nf.mnfst_val_icms,0)              != NVL(pt.mnfst_val_icms,0)
            OR NVL(nf.mnfst_val_isentas,0)           != NVL(pt.mnfst_val_isentas,0))		
        AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
	AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
        AND nf.mnfst_dtemiss >= TO_DATE('01/01/2016','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2016','DD/MM/YYYY')	
	    AND inf.emps_cod      = nf.emps_cod   
		AND inf.fili_cod      = nf.fili_cod   
		AND inf.infst_serie   = nf.mnfst_serie   
		AND inf.infst_num     = nf.mnfst_num   
		AND inf.infst_dtemiss = nf.mnfst_dtemiss
		AND EXISTS (SELECT 1 
		            FROM openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) inf1   
					WHERE inf1.emps_cod    = nf.emps_cod   
					AND   inf1.fili_cod      = nf.fili_cod   
					AND   inf1.infst_serie   = nf.mnfst_serie   
					AND   inf1.infst_num     = nf.mnfst_num   
					AND   inf1.infst_dtemiss = nf.mnfst_dtemiss
					AND ((NVL(inf1.infst_val_serv,0) - NVL(inf1.infst_val_desc,0) <> NVL(inf1.infst_base_icms,0) + NVL(inf1.infst_outras_icms,0) + NVL(inf1.infst_isenta_icms,0))
							OR (NVL(inf1.infst_val_cont,0) <> NVL(inf1.infst_base_icms,0) + NVL(inf1.infst_outras_icms,0) + NVL(inf1.infst_isenta_icms,0)))
					)
		ORDER BY nf.rowid;
		
   TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_sanea t_sanea;
   v_sanea    c_sanea%ROWTYPE;

   CURSOR c_cp( p_nm_processo gfcadastro.controle_processamento.nm_processo%type := NULL, p_dt_limite_inf_nf_ini gfcadastro.controle_processamento.dt_limite_inf_nf%type := NULL, p_dt_limite_inf_nf_fim gfcadastro.controle_processamento.dt_limite_inf_nf%type := NULL, p_rowid_cp rowid := NULL, p_st_processamento gfcadastro.controle_processamento.st_processamento%type := NULL )
    IS
    SELECT
      cp.rowid             AS ROWID_CP,
      cp.qt_atualizados_nf AS QT_PROCESSADOS,
      cp.*,
      CAST(0 AS NUMBER)                     AS UPDATE_REG,
      NVL(TRIM(cp.DS_FILTRO),'1=1')         AS FILTRO,
      NVL(TRIM(cp.DS_TRANSACAO),'ROLLBACK') AS TRANSACAO,
      UPPER(TRIM(TRANSLATE(REPLACE(','
      ||NVL(TRIM(REPLACE(cp.DS_REGRAS,'"','')),'N/A')
      || ',','''',''),'x ','x')))   AS REGRAS,
      TRIM(cp.DS_OUTROS_PARAMETROS) AS OUTROS_PARAMETROS
    FROM ${TABELA_CONTROLE} cp
    WHERE ((p_rowid_cp IS NULL OR NVL(LENGTH(TRIM(p_rowid_cp)),0) = 0) OR (cp.rowid = p_rowid_cp)) 
	AND ((p_nm_processo IS NULL OR NVL(LENGTH(TRIM(p_nm_processo)),0) = 0) OR (cp.nm_processo = p_nm_processo))
    AND ((p_dt_limite_inf_nf_ini IS NULL OR p_dt_limite_inf_nf_ini  < to_date('01/01/1980','DD/MM/YYYY')) OR (cp.dt_limite_inf_nf  >= p_dt_limite_inf_nf_ini))
    AND ((p_dt_limite_inf_nf_fim IS NULL OR p_dt_limite_inf_nf_fim < to_date('01/01/1980','DD/MM/YYYY')) OR (cp.dt_limite_inf_nf <= p_dt_limite_inf_nf_fim))
    AND ((p_st_processamento IS NULL OR NVL(LENGTH(TRIM(p_st_processamento)),0) = 0) OR (cp.st_processamento = p_st_processamento));
   v_cp c_cp%ROWTYPE;
   
   TYPE r_dml IS RECORD (
	   altera_inf          BOOLEAN := FALSE 
   );
   v_dml r_dml;
   
   TYPE r_chave_aux IS RECORD (
	   rowid_nf            ROWID         := NULL,
	   rowid_inf           ROWID         := NULL
   );
   v_chave_aux r_chave_aux;

   v_inf 	             openrisow.item_nftl_serv%rowtype;

   v_ds_etapa            VARCHAR2(4000);
   PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
   BEGIN
     v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
     DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
   EXCEPTION
     WHEN OTHERS THEN
	   NULL;
   END;
   
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_38(
    p_inf            IN OUT openrisow.item_nftl_serv%rowtype,
    p_mnfst_serie    IN openrisow.mestre_nftl_serv.mnfst_serie%type,
	p_altera_inf     IN OUT BOOLEAN)
AS
  v_serv_cod    openrisow.item_nftl_serv.serv_cod%type;
  v_cc_regra    CHAR(01);  
BEGIN

  IF ( p_mnfst_serie NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') AND p_mnfst_serie IS NOT NULL) 
       AND ( p_inf.infst_dtemiss >= to_date('01/01/2016','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') 
	  )
  THEN

    v_serv_cod               := upper(trim(TRANSLATE(p_inf.serv_cod,'x ','x')));
	v_serv_cod               := trim(substr(replace(replace(replace(replace(v_serv_cod,p_mnfst_serie||'ZP',''),p_mnfst_serie||'ZN',''),p_mnfst_serie||'Z',''),p_mnfst_serie,''),1,60));          

	SELECT
		CASE 
			WHEN NVL(p_inf.infst_val_desc,0) <> 0 AND NVL(p_inf.infst_val_serv,0) = NVL(p_inf.infst_val_desc,0) AND NVL(p_inf.infst_isenta_icms,0) <> 0 AND NVL(p_inf.infst_outras_icms,0) = 0 AND NVL(p_inf.infst_val_cont,0) = 0 AND NVL(p_inf.infst_base_icms,0) = 0 THEN '1'
			WHEN NVL(p_inf.infst_val_cont,0) = NVL(p_inf.infst_base_icms,0) AND NVL(p_inf.infst_val_cont,0) = NVL(p_inf.infst_val_serv,0) AND NVL(p_inf.infst_isenta_icms,0) <> 0 AND NVL(p_inf.infst_outras_icms,0) = 0 AND NVL(p_inf.infst_val_desc,0) = 0 THEN '2'
			WHEN NVL(p_inf.infst_isenta_icms,0) <> 0 AND NVL(p_inf.infst_base_icms,0) = 0 AND NVL(p_inf.infst_isenta_icms,0) <> NVL(p_inf.infst_val_cont,0) AND NVL(p_inf.infst_outras_icms,0) = 0 AND NVL(p_inf.infst_val_cont,0) = NVL(p_inf.infst_val_serv,0) AND NVL(p_inf.infst_val_desc,0) = 0 AND NVL(p_inf.infst_val_icms,0) = 0 AND NVL(p_inf.infst_outras_icms,0) = 0 AND NVL(p_inf.infst_val_icms,0) = 0 THEN '3'
			WHEN NVL(p_inf.infst_val_cont,0) = NVL(p_inf.infst_outras_icms,0) AND NVL(p_inf.infst_isenta_icms,0) <> 0 AND NVL(p_inf.infst_base_icms,0) = 0 AND NVL(p_inf.infst_val_icms,0) = 0 THEN '4'
			WHEN NVL(p_inf.infst_val_cont,0) = NVL(p_inf.infst_base_icms,0) AND NVL(p_inf.infst_val_cont,0) = (NVL(p_inf.infst_val_serv,0) - NVL(p_inf.infst_val_desc,0)) AND NVL(p_inf.infst_val_desc,0) <> 0 AND NVL(p_inf.infst_isenta_icms,0) <> 0 AND NVL(p_inf.infst_outras_icms,0) = 0 THEN '5'
			WHEN NVL(p_inf.infst_isenta_icms,0) <> 0 AND NVL(p_inf.infst_base_icms,0) = 0 AND NVL(p_inf.infst_isenta_icms,0) <> NVL(p_inf.infst_val_cont,0) AND NVL(p_inf.infst_outras_icms,0) = 0 AND NVL(p_inf.infst_val_cont,0) = (NVL(p_inf.infst_val_serv,0) - NVL(p_inf.infst_val_desc,0)) AND NVL(p_inf.infst_val_icms,0) = 0 THEN '6'
			ELSE '7' 
		END
		INTO v_cc_regra
	FROM DUAL;

-- Regra 1
-- nf.infst_val_desc <> 0 and nf.infst_val_serv = nf.infst_val_desc and nf.infst_isenta_icms <> 0 and nf.infst_outras_icms = 0 and nf.infst_val_cont = 0 and nf.infst_base_icms = 0 and nf.INFST_VAL_ICMS = 0
-- Zera o campo INFST_ISENTA_ICMS e altera o campo INFST_TRIBICMS = 'N', para os registros com alíquota, o campo INFST_ALIQ_ICMS será zerado.
	IF TRIM(v_cc_regra)   = '1' 
	THEN	
		-- -> Zera o campo INFST_ISENTA_ICMS e altera o campo INFST_TRIBICMS = 'N', para os registros com alíquota, o campo INFST_ALIQ_ICMS será zerado.
        p_altera_inf             := TRUE;
		p_inf.var05              := substr('r2016_38u<1>:' || p_inf.infst_isenta_icms || '|' || p_inf.infst_tribicms || '|' || p_inf.infst_aliq_icms || '>>' ||p_inf.VAR05,1,150) ;	 			
		p_inf.infst_isenta_icms  := 0;		  
		p_inf.infst_tribicms     := 'N';
		p_inf.infst_aliq_icms    := 0;	  
	END IF;
	
-- Regra 2
-- nf.infst_val_cont = nf.infst_base_icms and nf.infst_val_cont = nf.infst_val_serv and nf.infst_isenta_icms <> 0 and nf.infst_outras_icms = 0 and nf.infst_val_desc = 0
-- Zera o campo INFST_ISENTA_ICMS e altera o campo ESTB_COD = '00', INFST_TRIBICMS = 'S' e caso não tenha aliquota colocar 25
	IF TRIM(v_cc_regra)   = '2'
	THEN  
        p_altera_inf             := TRUE;
		p_inf.var05              := substr('r2016_38u<2>:' || p_inf.infst_isenta_icms || '|' || p_inf.estb_cod || '|' || p_inf.infst_tribicms || '|' || p_inf.infst_aliq_icms || '>>' ||p_inf.VAR05,1,150) ;	 			
		p_inf.infst_isenta_icms  := 0;	
		p_inf.estb_cod  		 := '00'; 	
		p_inf.infst_tribicms     := 'S';
		IF NVL((TO_CHAR(p_inf.infst_aliq_icms)),'_0_') = '_0_' THEN
			p_inf.infst_aliq_icms    := 25;
		END IF;	
	END IF;	

-- Regra 3
-- nf.infst_isenta_icms <> 0 and nf.infst_base_icms = 0 and nf.infst_isenta_icms <> nf.infst_val_cont and nf.infst_outras_icms = 0 and nf.infst_val_cont = nf.infst_val_serv and nf.infst_val_desc = 0 and nf.infst_val_icms = 0 and nf.infst_outras_icms = 0 and nf.INFST_VAL_ICMS = 0
-- Coloca o valor do campo INFST_VAL_CONT para o campo INFST_ISENTA_ICMS.
	IF TRIM(v_cc_regra)   = '3'
	THEN
        p_altera_inf             := TRUE;
		p_inf.var05              := substr('r2016_38u<3>:' || p_inf.infst_isenta_icms || '|' || p_inf.infst_val_cont  || '>>' ||p_inf.VAR05,1,150) ;	 			
		p_inf.infst_isenta_icms  := p_inf.infst_val_cont; 		
	END IF;
	
-- Regra 4
-- nf.infst_val_cont = nf.infst_outras_icms and nf.infst_isenta_icms <> 0 and nf.infst_base_icms = 0 and nf.infst_val_desc = 0 and nf.INFST_VAL_ICMS = 0
-- Zera o campo INFST_ISENTA_ICMS, e altera o ESTB_COD = 90 and INFST_TRIBICMS = 'P' para todo os registros. 
	IF TRIM(v_cc_regra)   = '4'
	THEN
		p_altera_inf             := TRUE;
		p_inf.var05              := substr('r2016_38u<4>:' || p_inf.infst_isenta_icms || '|' || p_inf.estb_cod || '|' || p_inf.infst_tribicms || '|' || p_inf.cfop || '>>' ||p_inf.VAR05,1,150) ;	 			
		p_inf.infst_isenta_icms  := 0;
		p_inf.estb_cod  		 := '90'; 
		p_inf.infst_tribicms     := 'P';
		IF (v_serv_cod IN ('3/12152','3/12191','3/12202','3/12253','3/12254','3/12500','3/12501','3/12848','3/12849','3/12850','3/12939', '7/925') 
	         OR p_inf.serv_cod IN ('3/12152','3/12191','3/12202','3/12253','3/12254','3/12500','3/12501','3/12848','3/12849','3/12850','3/12939', '7/925')) 
		THEN
			p_inf.cfop     			 := '0000';
		END IF;		
	END IF;
	
-- Regra 5
-- nf.infst_val_cont = nf.infst_base_icms and nf.infst_val_cont = (nf.infst_val_serv - nf.infst_val_desc) and nf.infst_val_desc <> 0 and nf.infst_isenta_icms <> 0 and nf.infst_outras_icms = 0
-- Zera o campo INFST_ISENTA_ICMS e altera o ESTB_COD para '00' and TRIBICMS = 'S'
	IF TRIM(v_cc_regra)   = '5'
    THEN
		p_altera_inf             := TRUE;
		p_inf.var05              := substr('r2016_38u<5>:' || p_inf.infst_isenta_icms || '|' || p_inf.estb_cod || '|' || p_inf.infst_tribicms  || '>>' ||p_inf.VAR05,1,150) ;	 			
		p_inf.infst_isenta_icms  := 0;
		p_inf.estb_cod  		 := '00';
		p_inf.infst_tribicms     := 'S';
	END IF;	
	
-- Regra 6
-- nf.infst_isenta_icms <> 0 and nf.infst_base_icms = 0 and nf.infst_isenta_icms <> nf.infst_val_cont and nf.infst_outras_icms = 0 and nf.infst_val_cont = (nf.infst_val_serv - nf.infst_val_desc) and nf.infst_val_icms = 0
-- Colocar o valor do campo INFST_VAL_CONT para o campo INFST_ISENTA_ICMS o ESTB_COD = 40 e  INFST_TRIBICMS = 'N'.
-- Para os registros com alíquota, o campo INFST_ALIQ_ICMS será zerado.
	IF TRIM(v_cc_regra)   = '6'
	THEN
        p_altera_inf             := TRUE;
		p_inf.var05              := substr('r2016_38u<6>:' || p_inf.infst_isenta_icms || '|' || p_inf.estb_cod || '|' || p_inf.infst_tribicms || '|' || p_inf.infst_aliq_icms || '>>' ||p_inf.VAR05,1,150) ;	 			
		p_inf.infst_isenta_icms  := p_inf.infst_val_cont;	
		p_inf.estb_cod  		 := '40';
		p_inf.infst_tribicms     := 'N';
		IF TRIM(NVL((TO_CHAR(p_inf.infst_aliq_icms)),'_0_')) != '_0_' THEN
			p_inf.infst_aliq_icms    := 0;
		END IF;	
	END IF;
	  
-- Regra 7 <<CANCELADO>>
-- Outros - Sem Alteração no campo Isentas
-- Para os serviços ('3/12152','3/12191','3/12202','3/12253','3/12254','3/12500','3/12501','3/12848','3/12849','3/12850','3/12939', '7/925') 
-- and INFST_VAL_CONT = INFST_VAL_SERV and INFST_VAL_DESC = 0 and INFST_BASE_ICMS = 0 and INFST_OUTRAS_ICMS = 0, alterar o ESTB_COD = 90 e INFST_TRIBICMS = 'P', e CFOP = '0000'..
	-- IF TRIM(v_cc_regra)   = '7'
	-- THEN
        -- p_altera_inf             := TRUE;
		-- p_inf.var05              := substr('r2016_38u<7>:' || p_inf.infst_val_cont     || '|' 
		--												   || p_inf.infst_val_serv     || '|' 
		--												   || p_inf.infst_val_desc     || '|' 
		--												   || p_inf.infst_base_icms    || '|'
		--												   || p_inf.infst_outras_icms  || '|'
		--												   || p_inf.estb_cod           || '|'
		--												   || p_inf.infst_tribicms     || '|'		
		--												   || p_inf.cfop  || '>>' ||p_inf.VAR05,1,150) ;	
														   
		-- IF v_serv_cod IN ('3/12152','3/12191','3/12202','3/12253','3/12254','3/12500','3/12501','3/12848','3/12849','3/12850','3/12939', '7/925') 
		--	AND (NVL(p_inf.infst_val_cont,0)     = NVL(p_inf.infst_val_serv,0)
		--	AND	 NVL(p_inf.infst_val_desc,0)     = 0
		--	AND	 NVL(p_inf.infst_base_icms,0)    = 0
		--	AND	 NVL(p_inf.infst_outras_icms,0)  = 0)		
		-- THEN
		--	p_inf.estb_cod  		 := '90';
		--	p_inf.infst_tribicms     := 'P';
		--	p_inf.cfop     			 := '0000';		
		-- END IF;
		
	-- END IF;

-- Se SERV_COD in('3/12152','3/12191','3/12202','3/12253','3/12254','3/12500','3/12501','3/12848','3/12849','3/12850','3/12939', '7/925') 
--    and INFST_VAL_CONT    = INFST_VAL_SERV 
--    and INFST_VAL_DESC    = 0 
--    and INFST_BASE_ICMS   = 0 
--    and INFST_VAL_ICMS    = 0 
--    and INFST_ALIQ_ICMS   = 0 
--    and INFST_OUTRAS_ICMS = 0:
-- Alterar o ESTB_COD = 90 e INFST_TRIBICMS = 'P' e CFOP = '0000', copiar o INFST_ISENTA_ICMS  para o INFST_OUTRAS_ICMS e zerar o INFST_ISENTA_ICMS.		
	IF      (v_serv_cod IN ('3/12152','3/12191','3/12202','3/12253','3/12254','3/12500','3/12501','3/12848','3/12849','3/12850','3/12939', '7/925') 
	         OR p_inf.serv_cod IN ('3/12152','3/12191','3/12202','3/12253','3/12254','3/12500','3/12501','3/12848','3/12849','3/12850','3/12939', '7/925'))
		AND (NVL(p_inf.infst_val_cont,0)     = NVL(p_inf.infst_val_serv,0)
			AND	 NVL(p_inf.infst_val_desc,0)     = 0
			AND	 NVL(p_inf.infst_base_icms,0)    = 0
			AND  NVL(p_inf.infst_val_icms,0)     = 0 
			AND  NVL(p_inf.infst_aliq_icms,0)    = 0
			AND	 NVL(p_inf.infst_outras_icms,0)  = 0)		
	THEN
        p_altera_inf             := TRUE;
		p_inf.var05              := substr('r2016_38u<0>:' || p_inf.infst_val_cont     || '|' 
														   || p_inf.infst_isenta_icms  || '|' 
														   || p_inf.infst_outras_icms  || '|'
														   || p_inf.estb_cod           || '|'
														   || p_inf.infst_tribicms     || '|'		
														   || p_inf.cfop  || '>>' ||p_inf.VAR05,1,150) ;		
		p_inf.estb_cod  		 := '90';
		p_inf.infst_tribicms     := 'P';
		p_inf.cfop     			 := '0000';	
		p_inf.infst_outras_icms	 := NVL(p_inf.infst_isenta_icms,0);
		p_inf.infst_isenta_icms  := 0; 	
	END IF;

  END IF;

END;
-- /
 
  
BEGIN
   
   -- Inicializacao
   prc_tempo('Inicializacao');
   v_dml.altera_inf :=  FALSE;

   -- CP
   v_cp.rowid_cp                := '${ROWID_CP}';
   prc_tempo('CP >> ' || v_cp.rowid_cp);  
   OPEN c_cp(p_ROWID_CP => v_cp.rowid_cp);
   FETCH c_cp INTO v_cp;
   IF c_cp%NOTFOUND THEN
      RAISE_APPLICATION_ERROR (-20343, 'Controle de Processamento nao encontrado!');
   END IF;
   CLOSE c_cp;
   ${COMMIT};
   
   -- IF v_cp.dt_limite_inf_nf = TRUNC(v_cp.dt_limite_inf_nf,'MM') THEN
   
	   prc_tempo('SANEA');
	   OPEN c_sanea;--(p_mnfst_dtemiss => v_cp.dt_limite_inf_nf);
	   LOOP
		  FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
		  :v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
		  IF v_bk_sanea.COUNT > 0 THEN
		  
			FOR i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST 
			LOOP	
				-- Atribuicao INF
				v_chave_aux.rowid_inf            := v_bk_sanea(i).rowid_inf;	
				v_inf.emps_cod                   := v_bk_sanea(i).emps_cod             ;
				v_inf.fili_cod                   := v_bk_sanea(i).fili_cod             ;
				v_inf.cgc_cpf                    := v_bk_sanea(i).cgc_cpf              ;
				v_inf.ie                         := v_bk_sanea(i).ie                   ;
				v_inf.uf                         := v_bk_sanea(i).uf                   ;
				v_inf.tp_loc                     := v_bk_sanea(i).tp_loc               ;
				v_inf.localidade                 := v_bk_sanea(i).localidade           ;
				v_inf.tdoc_cod                   := v_bk_sanea(i).tdoc_cod             ;
				v_inf.infst_serie                := v_bk_sanea(i).infst_serie          ;
				v_inf.infst_num                  := v_bk_sanea(i).infst_num            ;
				v_inf.infst_dtemiss              := v_bk_sanea(i).infst_dtemiss        ;
				v_inf.catg_cod                   := v_bk_sanea(i).catg_cod             ;
				v_inf.cadg_cod                   := v_bk_sanea(i).cadg_cod             ;
				v_inf.serv_cod                   := v_bk_sanea(i).serv_cod             ;
				v_inf.estb_cod                   := v_bk_sanea(i).estb_cod             ;
				v_inf.infst_dsc_compl            := v_bk_sanea(i).infst_dsc_compl      ;
				v_inf.infst_val_cont             := v_bk_sanea(i).infst_val_cont       ;
				v_inf.infst_val_serv             := v_bk_sanea(i).infst_val_serv       ;
				v_inf.infst_val_desc             := v_bk_sanea(i).infst_val_desc       ;
				v_inf.infst_aliq_icms            := v_bk_sanea(i).infst_aliq_icms      ;
				v_inf.infst_base_icms            := v_bk_sanea(i).infst_base_icms      ;
				v_inf.infst_val_icms             := v_bk_sanea(i).infst_val_icms       ;
				v_inf.infst_isenta_icms          := v_bk_sanea(i).infst_isenta_icms    ;
				v_inf.infst_outras_icms          := v_bk_sanea(i).infst_outras_icms    ;
				v_inf.infst_tribipi              := v_bk_sanea(i).infst_tribipi        ;
				v_inf.infst_tribicms             := v_bk_sanea(i).infst_tribicms       ;
				v_inf.infst_isenta_ipi           := v_bk_sanea(i).infst_isenta_ipi     ;
				v_inf.infst_outra_ipi            := v_bk_sanea(i).infst_outra_ipi      ;
				v_inf.infst_outras_desp          := v_bk_sanea(i).infst_outras_desp    ;
				v_inf.infst_fiscal               := v_bk_sanea(i).infst_fiscal         ;
				v_inf.infst_num_seq              := v_bk_sanea(i).infst_num_seq        ;
				v_inf.infst_tel                  := v_bk_sanea(i).infst_tel            ;
				v_inf.infst_ind_canc             := v_bk_sanea(i).infst_ind_canc       ;
				v_inf.infst_proter               := v_bk_sanea(i).infst_proter         ;
				v_inf.infst_cod_cont             := v_bk_sanea(i).infst_cod_cont       ;
				v_inf.cfop                       := v_bk_sanea(i).cfop                 ;
				v_inf.mdoc_cod                   := v_bk_sanea(i).mdoc_cod             ;
				v_inf.cod_prest                  := v_bk_sanea(i).cod_prest            ;
				v_inf.num01                      := v_bk_sanea(i).num01                ;
				v_inf.num02                      := v_bk_sanea(i).num02                ;
				v_inf.num03                      := v_bk_sanea(i).num03                ;
				v_inf.var01                      := v_bk_sanea(i).var01                ;
				v_inf.var02                      := v_bk_sanea(i).var02                ;
				v_inf.var03                      := v_bk_sanea(i).var03                ;
				v_inf.var04                      := v_bk_sanea(i).var04                ;
				v_inf.var05                      := v_bk_sanea(i).var05                ;
				v_inf.infst_ind_cnv115           := v_bk_sanea(i).infst_ind_cnv115     ;
				v_inf.infst_unid_medida          := v_bk_sanea(i).infst_unid_medida    ;
				v_inf.infst_quant_contr          := v_bk_sanea(i).infst_quant_contr    ;
				v_inf.infst_quant_prest          := v_bk_sanea(i).infst_quant_prest    ;
				v_inf.infst_codh_reg             := v_bk_sanea(i).infst_codh_reg       ;
				v_inf.esta_cod                   := v_bk_sanea(i).esta_cod             ;
				v_inf.infst_val_pis              := v_bk_sanea(i).infst_val_pis        ;
				v_inf.infst_val_cofins           := v_bk_sanea(i).infst_val_cofins     ;
				v_inf.infst_bas_icms_st          := v_bk_sanea(i).infst_bas_icms_st    ;
				v_inf.infst_aliq_icms_st         := v_bk_sanea(i).infst_aliq_icms_st   ;
				v_inf.infst_val_icms_st          := v_bk_sanea(i).infst_val_icms_st    ;
				v_inf.infst_val_red              := v_bk_sanea(i).infst_val_red        ;
				v_inf.tpis_cod                   := v_bk_sanea(i).tpis_cod             ;
				v_inf.tcof_cod                   := v_bk_sanea(i).tcof_cod             ;
				v_inf.infst_bas_piscof           := v_bk_sanea(i).infst_bas_piscof     ;
				v_inf.infst_aliq_pis             := v_bk_sanea(i).infst_aliq_pis       ;
				v_inf.infst_aliq_cofins          := v_bk_sanea(i).infst_aliq_cofins    ;
				v_inf.infst_nat_rec              := v_bk_sanea(i).infst_nat_rec        ;
				v_inf.cscp_cod                   := v_bk_sanea(i).cscp_cod             ;
				v_inf.infst_num_contr            := v_bk_sanea(i).infst_num_contr      ;
				v_inf.infst_tip_isencao          := v_bk_sanea(i).infst_tip_isencao    ;
				v_inf.infst_tar_aplic            := v_bk_sanea(i).infst_tar_aplic      ;
				v_inf.infst_ind_desc             := v_bk_sanea(i).infst_ind_desc       ;
				v_inf.infst_num_fat              := v_bk_sanea(i).infst_num_fat        ;
				v_inf.infst_qtd_fat              := v_bk_sanea(i).infst_qtd_fat        ;
				v_inf.infst_mod_ativ             := v_bk_sanea(i).infst_mod_ativ       ;
				v_inf.infst_hora_ativ            := v_bk_sanea(i).infst_hora_ativ      ;
				v_inf.infst_id_equip             := v_bk_sanea(i).infst_id_equip       ;
				v_inf.infst_mod_pgto             := v_bk_sanea(i).infst_mod_pgto       ;
				v_inf.infst_num_nfe              := v_bk_sanea(i).infst_num_nfe        ;
				v_inf.infst_dtemiss_nfe          := v_bk_sanea(i).infst_dtemiss_nfe    ;
				v_inf.infst_val_cred_nfe         := v_bk_sanea(i).infst_val_cred_nfe   ;
				v_inf.infst_cnpj_can_com         := v_bk_sanea(i).infst_cnpj_can_com   ;
				v_inf.infst_val_desc_pis         := v_bk_sanea(i).infst_val_desc_pis   ;
				v_inf.infst_val_desc_cofins      := v_bk_sanea(i).infst_val_desc_cofins;			
				IF v_chave_aux.rowid_nf IS NULL OR v_chave_aux.rowid_nf != v_bk_sanea(i).rowid_nf THEN
					-- Atribuicao NF
					v_chave_aux.rowid_nf         := v_bk_sanea(i).rowid_nf;
				END IF;
				
				-- <<INICIO TRATAMENTO INF>>
				
				-- IF   INSTR(v_cp.REGRAS,',' || 'r2016_38' || ',') > 0  OR INSTR(v_cp.REGRAS,',' || 'ALL' || ',') > 0 THEN
				prcts_regra_38(p_inf            => v_inf,
							   p_mnfst_serie    => v_bk_sanea(i).serie,
							   p_altera_inf     => v_dml.altera_inf);
				-- END IF;
				
				-- <<FIM TRATAMENTO INF>>		
				
				-- DML INF
				IF v_dml.altera_inf THEN
					UPDATE openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf 
					SET inf.cgc_cpf                    = v_inf.cgc_cpf              ,
						inf.ie                         = v_inf.ie                   ,
						inf.uf                         = v_inf.uf                   ,
						inf.tp_loc                     = v_inf.tp_loc               ,
						inf.localidade                 = v_inf.localidade           ,
						inf.tdoc_cod                   = v_inf.tdoc_cod             ,
						inf.catg_cod                   = v_inf.catg_cod             ,
						inf.cadg_cod                   = v_inf.cadg_cod             ,
						inf.serv_cod                   = v_inf.serv_cod             ,
						inf.estb_cod                   = v_inf.estb_cod             ,
						inf.infst_dsc_compl            = v_inf.infst_dsc_compl      ,
						inf.infst_val_cont             = v_inf.infst_val_cont       ,
						inf.infst_val_serv             = v_inf.infst_val_serv       ,
						inf.infst_val_desc             = v_inf.infst_val_desc       ,
						inf.infst_aliq_icms            = v_inf.infst_aliq_icms      ,
						inf.infst_base_icms            = v_inf.infst_base_icms      ,
						inf.infst_val_icms             = v_inf.infst_val_icms       ,
						inf.infst_isenta_icms          = v_inf.infst_isenta_icms    ,
						inf.infst_outras_icms          = v_inf.infst_outras_icms    ,
						inf.infst_tribipi              = v_inf.infst_tribipi        ,
						inf.infst_tribicms             = v_inf.infst_tribicms       ,
						inf.infst_isenta_ipi           = v_inf.infst_isenta_ipi     ,
						inf.infst_outra_ipi            = v_inf.infst_outra_ipi      ,
						inf.infst_outras_desp          = v_inf.infst_outras_desp    ,
						inf.infst_fiscal               = v_inf.infst_fiscal         ,
						inf.infst_num_seq              = v_inf.infst_num_seq        ,
						inf.infst_tel                  = v_inf.infst_tel            ,
						inf.infst_ind_canc             = v_inf.infst_ind_canc       ,
						inf.infst_proter               = v_inf.infst_proter         ,
						inf.infst_cod_cont             = v_inf.infst_cod_cont       ,
						inf.cfop                       = v_inf.cfop                 ,
						inf.mdoc_cod                   = v_inf.mdoc_cod             ,
						inf.cod_prest                  = v_inf.cod_prest            ,
						inf.num01                      = v_inf.num01                ,
						inf.num02                      = v_inf.num02                ,
						inf.num03                      = v_inf.num03                ,
						inf.var01                      = v_inf.var01                ,
						inf.var02                      = v_inf.var02                ,
						inf.var03                      = v_inf.var03                ,
						inf.var04                      = v_inf.var04                ,
						inf.var05                      = v_inf.var05                ,
						inf.infst_ind_cnv115           = v_inf.infst_ind_cnv115     ,
						inf.infst_unid_medida          = v_inf.infst_unid_medida    ,
						inf.infst_quant_contr          = v_inf.infst_quant_contr    ,
						inf.infst_quant_prest          = v_inf.infst_quant_prest    ,
						inf.infst_codh_reg             = v_inf.infst_codh_reg       ,
						inf.esta_cod                   = v_inf.esta_cod             ,
						inf.infst_val_pis              = v_inf.infst_val_pis        ,
						inf.infst_val_cofins           = v_inf.infst_val_cofins     ,
						inf.infst_bas_icms_st          = v_inf.infst_bas_icms_st    ,
						inf.infst_aliq_icms_st         = v_inf.infst_aliq_icms_st   ,
						inf.infst_val_icms_st          = v_inf.infst_val_icms_st    ,
						inf.infst_val_red              = v_inf.infst_val_red        ,
						inf.tpis_cod                   = v_inf.tpis_cod             ,
						inf.tcof_cod                   = v_inf.tcof_cod             ,
						inf.infst_bas_piscof           = v_inf.infst_bas_piscof     ,
						inf.infst_aliq_pis             = v_inf.infst_aliq_pis       ,
						inf.infst_aliq_cofins          = v_inf.infst_aliq_cofins    ,
						inf.infst_nat_rec              = v_inf.infst_nat_rec        ,
						inf.cscp_cod                   = v_inf.cscp_cod             ,
						inf.infst_num_contr            = v_inf.infst_num_contr      ,
						inf.infst_tip_isencao          = v_inf.infst_tip_isencao    ,
						inf.infst_tar_aplic            = v_inf.infst_tar_aplic      ,
						inf.infst_ind_desc             = v_inf.infst_ind_desc       ,
						inf.infst_num_fat              = v_inf.infst_num_fat        ,
						inf.infst_qtd_fat              = v_inf.infst_qtd_fat        ,
						inf.infst_mod_ativ             = v_inf.infst_mod_ativ       ,
						inf.infst_hora_ativ            = v_inf.infst_hora_ativ      ,
						inf.infst_id_equip             = v_inf.infst_id_equip       ,
						inf.infst_mod_pgto             = v_inf.infst_mod_pgto       ,
						inf.infst_num_nfe              = v_inf.infst_num_nfe        ,
						inf.infst_dtemiss_nfe          = v_inf.infst_dtemiss_nfe    ,
						inf.infst_val_cred_nfe         = v_inf.infst_val_cred_nfe   ,
						inf.infst_cnpj_can_com         = v_inf.infst_cnpj_can_com   ,
						inf.infst_val_desc_pis         = v_inf.infst_val_desc_pis   ,
						inf.infst_val_desc_cofins      = v_inf.infst_val_desc_cofins	
					WHERE inf.rowid = v_chave_aux.rowid_inf;
					:v_qtd_atu_inf   := :v_qtd_atu_inf + 1;
					v_dml.altera_inf :=  FALSE;
				END IF;	
						
			END LOOP;    
		  END IF;       
		  ${COMMIT};	
		  EXIT WHEN c_sanea%NOTFOUND;	  
	   END LOOP;        
	   CLOSE c_sanea;   
	   
   -- END IF;
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_processados || ' | NF : ' || :v_qtd_atu_nf || ' | INF : ' || :v_qtd_atu_inf|| ' | CLI : ' || :v_qtd_atu_cli);
EXCEPTION           
   WHEN OTHERS THEN 
      ROLLBACK;     
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
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
       cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp
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

