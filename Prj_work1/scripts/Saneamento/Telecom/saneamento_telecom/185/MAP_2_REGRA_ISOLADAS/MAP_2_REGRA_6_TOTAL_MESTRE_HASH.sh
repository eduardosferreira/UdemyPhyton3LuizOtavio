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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_6_TOTAL_MESTRE_HASH.sh'
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
PROMPT MAP_2_REGRA_6_TOTAL_MESTRE_HASH.sh
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT
DECLARE

	CONSTANTE_LIMIT PLS_INTEGER := 250000; 
  
    -- Cursores 
	CURSOR c_f
    IS
    SELECT * FROM openrisow.filial;
	
    CURSOR c_sanea
    IS
	SELECT  /*+ parallel(15) */    
		-- CASE lead(nf.catg_cod   || '|' || nf.cadg_cod , 1) over (ORDER BY nf.catg_cod   || '|' || nf.cadg_cod ) 	WHEN nf.catg_cod   || '|' || nf.cadg_cod   THEN 'N'	ELSE 'S' END AS last_reg_cli,  
		nf.catg_cod   || '|' || nf.cadg_cod AS CLI,     
		CASE LEAD(nf.rowid, 1) over (ORDER BY  nf.catg_cod   || '|' || nf.cadg_cod, nf.rowid)    
			WHEN nf.rowid    
			THEN 'N'    
			ELSE 'S'     
		END AS last_reg_nf,     
		upper(trim(TRANSLATE(nf.mnfst_serie,'x ','x'))) serie,     
		nf.rowid    AS rowid_nf,  
		nf.mnfst_num,
		nf.catg_cod AS mnfst_catg_cod,     
		nf.cadg_cod AS mnfst_cadg_cod,     
		nf.emps_cod AS mnfst_emps_cod,     
		nf.fili_cod AS mnfst_fili_cod,     
		nf.tdoc_cod AS mnfst_tdoc_cod,     
		nf.mnfst_serie ,     
		nf.mnfst_dtemiss ,     
		nf.mnfst_ind_cont ,     
		TRIM(TO_CHAR(COUNT(1) OVER (PARTITION BY nf.rowid),'00000')) AS mnfst_ind_cont_aux,
		nf.mdoc_cod AS mnfst_mdoc_cod,     
		nf.mnfst_val_tot ,     
		nf.mnfst_val_desc ,     
		nf.mnfst_ind_canc ,     
		nf.mnfst_dat_venc ,     
		nf.mnfst_per_ref ,     
		nf.mnfst_avista ,     
		nf.num01 AS mnfst_num01,     
		nf.num02 AS mnfst_num02,     
		nf.num03 AS mnfst_num03,     
		nf.var01 AS mnfst_var01,     
		nf.var02 AS mnfst_var02,     
		nf.var03 AS mnfst_var03,     
		nf.var04 AS mnfst_var04,     
		nf.var05 AS mnfst_var05,     
		nf.mnfst_ind_cnv115 ,     
		nf.cnpj_cpf AS mnfst_cnpj_cpf,     
		nf.mnfst_val_basicms ,     
		nf.mnfst_val_icms ,     
		nf.mnfst_val_isentas ,     
		nf.mnfst_val_outras ,     
		nf.mnfst_codh_nf ,     
		nf.mnfst_codh_regnf ,     
		nf.mnfst_codh_regcli ,     
		nf.mnfst_reg_esp ,     
		nf.mnfst_bas_icms_st ,     
		nf.mnfst_val_icms_st ,     
		nf.mnfst_val_pis ,     
		nf.mnfst_val_cofins ,     
		nf.mnfst_val_da ,     
		nf.mnfst_val_ser ,     
		nf.mnfst_val_terc ,     
		nf.cicd_cod_inf AS mnfst_cicd_cod_inf,     
		nf.mnfst_tip_assi ,     
		nf.mnfst_tip_util ,     
		nf.mnfst_grp_tens ,     
		nf.mnfst_ind_extemp ,     
		nf.mnfst_dat_extemp ,     
		nf.mnfst_num_fic ,     
		nf.mnfst_dt_lt_ant ,     
		nf.mnfst_dt_lt_atu ,     
		nf.mnfst_num_fat ,     
		nf.mnfst_vl_tot_fat ,     
		nf.mnfst_chv_nfe ,     
		nf.mnfst_dat_aut_nfe ,     
		nf.mnfst_val_desc_pis ,     
		nf.mnfst_val_desc_cofins ,     
		inf.rowid AS rowid_inf,     
		inf.emps_cod ,     
		inf.fili_cod ,     
		inf.cgc_cpf ,     
		inf.ie ,     
		inf.uf ,     
		inf.tp_loc ,     
		inf.localidade ,     
		inf.tdoc_cod ,     
		inf.infst_serie ,     
		inf.infst_num ,     
		inf.infst_dtemiss ,     
		inf.catg_cod ,     
		inf.cadg_cod ,     
		inf.serv_cod ,     
		inf.estb_cod ,     
		inf.infst_dsc_compl ,     
		inf.infst_val_cont ,     
		inf.infst_val_serv ,     
		inf.infst_val_desc ,     
		inf.infst_aliq_icms ,     
		inf.infst_bASe_icms ,     
		inf.infst_val_icms ,     
		inf.infst_isenta_icms ,     
		inf.infst_outrAS_icms ,     
		inf.infst_tribipi ,     
		inf.infst_tribicms ,     
		inf.infst_isenta_ipi ,     
		inf.infst_outra_ipi ,     
		inf.infst_outrAS_desp ,     
		inf.infst_fiscal ,     
		inf.infst_num_seq ,
		NULLIF(ROW_NUMBER() over(PARTITION BY nf.rowid ORDER BY CASE inf.infst_num_seq WHEN 0 THEN NULL ELSE inf.infst_num_seq END NULLs LAST),inf.infst_num_seq) infst_num_seq_aux ,     
		MAX(inf.infst_num_seq) OVER ( PARTITION BY nf.rowid ) infst_num_seq_max,     
		inf.infst_tel ,     
		inf.infst_ind_canc ,     
		inf.infst_proter ,     
		inf.infst_cod_cont ,     
		inf.cfop ,  
	    MAX(inf.cfop) OVER ( PARTITION BY nf.rowid ) cfop_max,	
		MIN(inf.cfop) OVER ( PARTITION BY nf.rowid ) cfop_min,			
		inf.mdoc_cod ,     
		inf.cod_prest ,     
		inf.num01 ,     
		inf.num02 ,     
		inf.num03 ,     
		inf.var01 ,     
		inf.var02 ,     
		inf.var03 ,     
		inf.var04 ,     
		inf.var05 ,     
		inf.infst_ind_cnv115 ,     
		inf.infst_unid_medida ,     
		inf.infst_quant_contr ,     
		inf.infst_quant_prest ,     
		inf.infst_codh_reg ,     
		inf.esta_cod ,     
		inf.infst_val_pis ,     
		inf.infst_val_cofins ,     
		inf.infst_bas_icms_st ,     
		inf.infst_aliq_icms_st ,     
		inf.infst_val_icms_st ,     
		inf.infst_val_red ,     
		inf.tpis_cod ,     
		inf.tcof_cod ,     
		inf.infst_bas_piscof ,     
		inf.infst_aliq_pis ,     
		inf.infst_aliq_cofins ,     
		inf.infst_nat_rec ,     
		inf.cscp_cod ,     
		inf.infst_num_contr ,     
		inf.infst_tip_isencao ,     
		inf.infst_tar_aplic ,     
		inf.infst_ind_desc ,     
		inf.infst_num_fat ,     
		inf.infst_qtd_fat ,     
		inf.infst_mod_ativ ,     
		inf.infst_hora_ativ ,     
		inf.infst_id_equip ,     
		inf.infst_mod_pgto ,     
		inf.infst_num_nfe ,     
		inf.infst_dtemiss_nfe ,     
		inf.infst_val_cred_nfe ,     
		inf.infst_cnpj_can_com ,     
		inf.infst_val_desc_pis ,     
		inf.infst_val_desc_cofins   
	FROM openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf,     
	     openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf   
	WHERE ${FILTRO} 
	    AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
	   AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
        AND inf.emps_cod      = nf.emps_cod   
		AND inf.fili_cod      = nf.fili_cod   
		AND inf.infst_serie   = nf.mnfst_serie   
		AND inf.infst_num     = nf.mnfst_num   
		AND inf.infst_dtemiss = nf.mnfst_dtemiss   
	ORDER BY -- CLI , LAST_REG_CLI , 
	         ROWID_NF
		   , LAST_REG_NF;	
    
	c_st_i VARCHAR2(150);
	
	TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
    v_bk_sanea t_sanea;
    v_sanea    c_sanea%ROWTYPE;
	
	TYPE c_f_t IS TABLE OF c_f%ROWTYPE INDEX BY c_st_i%TYPE;
    v_bk_f     c_f_t;
	v_f        c_f%ROWTYPE;
   
	TYPE r_dml IS RECORD (
	   merge_st            BOOLEAN := FALSE,
	   altera_st           BOOLEAN := FALSE,
	   altera_cli          BOOLEAN := FALSE,
	   altera_inf          BOOLEAN := FALSE,  
	   altera_nf           BOOLEAN := FALSE 
    );
    v_dml r_dml;
   
    TYPE r_chave_aux IS RECORD (
	   cliente             VARCHAR2(150) := NULL,
	   rowid_nf            ROWID         := NULL,
	   rowid_inf           ROWID         := NULL
    );
    v_chave_aux r_chave_aux;

    v_inf 	             openrisow.item_nftl_serv%rowtype;
    v_nf 	             openrisow.mestre_nftl_serv%rowtype;
    v_infst_num_seq_max   openrisow.item_nftl_serv.infst_num_seq%type := 0;   
    v_nr_qtde_inf         PLS_INTEGER;            
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
PROCEDURE prcts_regra_6(
    p_f              IN c_f%ROWTYPE,
    p_inf            IN openrisow.item_nftl_serv%rowtype,
    p_nf             IN OUT openrisow.mestre_nftl_serv%rowtype,
    p_mnfst_serie    IN openrisow.mestre_nftl_serv.mnfst_serie%type    := NULL,
    p_mnfst_ind_cont IN openrisow.mestre_nftl_serv.mnfst_ind_cont%type := NULL,
    p_nr_qtde_inf    IN PLS_INTEGER                                    := 0,
    p_last_reg       IN VARCHAR2                                       := 'N',
    p_altera_nf      IN OUT BOOLEAN)
AS
  -- 6) - Totalizacao do Mestre
BEGIN
  IF (p_mnfst_serie IS NOT NULL) 
  -- AND ( p_inf.infst_dtemiss >= to_date('01/01/2015','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2017','dd/mm/yyyy') ) 
  THEN

	-- (*) Resumarizar os totais do mestre
	IF p_nr_qtde_inf <= 1 THEN
		p_nf.var05               := SUBSTR('rMAP2_6:' || p_nf.mnfst_val_tot || '|' || p_nf.mnfst_val_isentas || '|' || p_nf.mnfst_val_outras || '|' || p_nf.mnfst_val_ser || '|' || p_nf.mnfst_val_icms || '|' || p_nf.mnfst_val_basicms || '|' || p_nf.mnfst_val_desc || '|' || p_nf.mnfst_codh_nf || '|' || p_nf.mnfst_ind_cont || '>>' ||p_nf.var05,1,150);
		p_nf.mnfst_val_tot       := 0;
		p_nf.mnfst_val_isentas   := 0;
		p_nf.mnfst_val_outras    := 0;
		p_nf.mnfst_val_ser       := 0;
		p_nf.mnfst_val_icms      := 0;
		p_nf.mnfst_val_basicms   := 0;
		p_nf.mnfst_val_desc      := 0;
		p_nf.mnfst_codh_nf       := NULL;
		p_nf.mnfst_ind_cont      := NULL;
	END IF;
	-- if p_inf.infst_ind_canc = 'N' THEN
		p_nf.mnfst_val_tot        := NVL(p_nf.mnfst_val_tot,0)     + NVL(p_inf.infst_val_cont,0);
		p_nf.mnfst_val_isentas    := NVL(p_nf.mnfst_val_isentas,0) + NVL(p_inf.infst_isenta_icms,0);
		p_nf.mnfst_val_outras     := NVL(p_nf.mnfst_val_outras,0)  + NVL(p_inf.infst_outras_icms,0);
		p_nf.mnfst_val_ser        := NVL(p_nf.mnfst_val_ser,0)     + NVL(p_inf.infst_val_serv,0);
		p_nf.mnfst_val_icms       := NVL(p_nf.mnfst_val_icms,0)    + NVL(p_inf.infst_val_icms,0);
		p_nf.mnfst_val_basicms    := NVL(p_nf.mnfst_val_basicms,0) + NVL(p_inf.infst_base_icms,0);
		p_nf.mnfst_val_desc       := NVL(p_nf.mnfst_val_desc,0)    + NVL(p_inf.infst_val_desc,0);
	-- END IF;
	IF UPPER(TRIM(p_last_reg)) = 'S' THEN
	    IF ( p_inf.infst_dtemiss >= to_date('01/01/2015','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') ) THEN
			p_nf.mnfst_codh_nf      := rawtohex(dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw(lpad(p_nf.cnpj_cpf, 14, 0) || lpad(p_nf.mnfst_num, 9, 0) || lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_tot, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_basicms, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_icms, '099999999.99'), '.', ''), ',', '')), 12, 0))));
		ELSE
			p_nf.mnfst_codh_nf      := rawtohex(dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw(lpad(p_nf.cnpj_cpf, 14, 0) || lpad(p_nf.mnfst_num, 9, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_tot, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_basicms, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_icms, '099999999.99'), '.', ''), ',', '')), 12, 0)||to_char(p_nf.mnfst_dtemiss, 'yyyymmdd')|| lpad(p_f.fili_cod_cgc, 14, 0))));
		END IF;
		p_nf.mnfst_ind_cont     := p_mnfst_ind_cont;
		p_altera_nf             := TRUE;
	END IF;
    
  END IF;
END;
-- /
 
  
BEGIN
   
   -- Inicializacao
   prc_tempo('Inicializacao');
   v_dml.altera_inf :=  FALSE;
   v_dml.altera_nf  :=  FALSE;
 
   FOR i IN c_f LOOP
	  v_bk_f(TO_CHAR(i.emps_cod)|| '|' ||TO_CHAR(i.fili_cod)) := i;
   END LOOP;
  
   prc_tempo('SANEA');
   OPEN c_sanea;--(p_mnfst_dtemiss => v_cp.dt_limite_inf_nf);
   LOOP
	  FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
	  :v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
	  IF v_bk_sanea.COUNT > 0 THEN
	  
		FOR i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST 
		LOOP	
			-- Atribuicao INF
			v_infst_num_seq_max              := v_bk_sanea(i).infst_num_seq_max;
			v_nr_qtde_inf                    := v_nr_qtde_inf + 1; 
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
				v_infst_num_seq_max          := v_bk_sanea(i).infst_num_seq_max;
				v_nr_qtde_inf                := 1;
				v_chave_aux.rowid_nf         := v_bk_sanea(i).rowid_nf;
				v_nf.EMPS_COD                := v_bk_sanea(i).MNFST_EMPS_COD       ;
				v_nf.FILI_COD                := v_bk_sanea(i).MNFST_FILI_COD       ;
				v_nf.TDOC_COD                := v_bk_sanea(i).MNFST_TDOC_COD       ;
				v_nf.MNFST_SERIE             := v_bk_sanea(i).MNFST_SERIE          ;
				v_nf.MNFST_NUM               := v_bk_sanea(i).MNFST_NUM            ;
				v_nf.MNFST_DTEMISS           := v_bk_sanea(i).MNFST_DTEMISS        ;
				v_nf.CATG_COD                := v_bk_sanea(i).MNFST_CATG_COD       ;
				v_nf.CADG_COD                := v_bk_sanea(i).MNFST_CADG_COD       ;
				v_nf.MNFST_IND_CONT          := v_bk_sanea(i).MNFST_IND_CONT       ;
				v_nf.MDOC_COD                := v_bk_sanea(i).MNFST_MDOC_COD       ;
				v_nf.MNFST_VAL_TOT           := v_bk_sanea(i).MNFST_VAL_TOT        ;
				v_nf.MNFST_VAL_DESC          := v_bk_sanea(i).MNFST_VAL_DESC       ;
				v_nf.MNFST_IND_CANC          := v_bk_sanea(i).MNFST_IND_CANC       ;
				v_nf.MNFST_DAT_VENC          := v_bk_sanea(i).MNFST_DAT_VENC       ;
				v_nf.MNFST_PER_REF           := v_bk_sanea(i).MNFST_PER_REF        ;
				v_nf.MNFST_AVISTA            := v_bk_sanea(i).MNFST_AVISTA         ;
				v_nf.NUM01                   := v_bk_sanea(i).MNFST_NUM01          ;
				v_nf.NUM02                   := v_bk_sanea(i).MNFST_NUM02          ;
				v_nf.NUM03                   := v_bk_sanea(i).MNFST_NUM03          ;
				v_nf.VAR01                   := v_bk_sanea(i).MNFST_VAR01          ;
				v_nf.VAR02                   := v_bk_sanea(i).MNFST_VAR02          ;
				v_nf.VAR03                   := v_bk_sanea(i).MNFST_VAR03          ;
				v_nf.VAR04                   := v_bk_sanea(i).MNFST_VAR04          ;
				v_nf.VAR05                   := v_bk_sanea(i).MNFST_VAR05          ;
				v_nf.MNFST_IND_CNV115        := v_bk_sanea(i).MNFST_IND_CNV115     ;
				v_nf.CNPJ_CPF                := v_bk_sanea(i).MNFST_CNPJ_CPF       ;
				v_nf.MNFST_VAL_BASICMS       := v_bk_sanea(i).MNFST_VAL_BASICMS    ;
				v_nf.MNFST_VAL_ICMS          := v_bk_sanea(i).MNFST_VAL_ICMS       ;
				v_nf.MNFST_VAL_ISENTAS       := v_bk_sanea(i).MNFST_VAL_ISENTAS    ;
				v_nf.MNFST_VAL_OUTRAS        := v_bk_sanea(i).MNFST_VAL_OUTRAS     ;
				v_nf.MNFST_CODH_NF           := v_bk_sanea(i).MNFST_CODH_NF        ;
				v_nf.MNFST_CODH_REGNF        := v_bk_sanea(i).MNFST_CODH_REGNF     ;
				v_nf.MNFST_CODH_REGCLI       := v_bk_sanea(i).MNFST_CODH_REGCLI    ;
				v_nf.MNFST_REG_ESP           := v_bk_sanea(i).MNFST_REG_ESP        ;
				v_nf.MNFST_BAS_ICMS_ST       := v_bk_sanea(i).MNFST_BAS_ICMS_ST    ;
				v_nf.MNFST_VAL_ICMS_ST       := v_bk_sanea(i).MNFST_VAL_ICMS_ST    ;
				v_nf.MNFST_VAL_PIS           := v_bk_sanea(i).MNFST_VAL_PIS        ;
				v_nf.MNFST_VAL_COFINS        := v_bk_sanea(i).MNFST_VAL_COFINS     ;
				v_nf.MNFST_VAL_DA            := v_bk_sanea(i).MNFST_VAL_DA         ;
				v_nf.MNFST_VAL_SER           := v_bk_sanea(i).MNFST_VAL_SER        ;
				v_nf.MNFST_VAL_TERC          := v_bk_sanea(i).MNFST_VAL_TERC       ;
				v_nf.CICD_COD_INF            := v_bk_sanea(i).MNFST_CICD_COD_INF   ;
				v_nf.MNFST_TIP_ASSI          := v_bk_sanea(i).MNFST_TIP_ASSI       ;
				v_nf.MNFST_TIP_UTIL          := v_bk_sanea(i).MNFST_TIP_UTIL       ;
				v_nf.MNFST_GRP_TENS          := v_bk_sanea(i).MNFST_GRP_TENS       ;
				v_nf.MNFST_IND_EXTEMP        := v_bk_sanea(i).MNFST_IND_EXTEMP     ;
				v_nf.MNFST_DAT_EXTEMP        := v_bk_sanea(i).MNFST_DAT_EXTEMP     ;
				v_nf.MNFST_NUM_FIC           := v_bk_sanea(i).MNFST_NUM_FIC        ;
				v_nf.MNFST_DT_LT_ANT         := v_bk_sanea(i).MNFST_DT_LT_ANT      ;
				v_nf.MNFST_DT_LT_ATU         := v_bk_sanea(i).MNFST_DT_LT_ATU      ;
				v_nf.MNFST_NUM_FAT           := v_bk_sanea(i).MNFST_NUM_FAT        ;
				v_nf.MNFST_VL_TOT_FAT        := v_bk_sanea(i).MNFST_VL_TOT_FAT     ;
				v_nf.MNFST_CHV_NFE           := v_bk_sanea(i).MNFST_CHV_NFE        ;
				v_nf.MNFST_DAT_AUT_NFE       := v_bk_sanea(i).MNFST_DAT_AUT_NFE    ;
				v_nf.MNFST_VAL_DESC_PIS      := v_bk_sanea(i).MNFST_VAL_DESC_PIS   ;
				v_nf.MNFST_VAL_DESC_COFINS 	 := v_bk_sanea(i).MNFST_VAL_DESC_COFINS;
				BEGIN
					v_f :=  v_bk_f(TO_CHAR(v_nf.emps_cod)|| '|' ||TO_CHAR(v_nf.fili_cod));
				EXCEPTION
				WHEN OTHERS THEN
					NULL;
				END;
			END IF;
			
			-- <<INICIO TRATAMENTO INF>>
			
			prcts_regra_6(p_f                      => v_f,
						  p_inf                    => v_inf,
						  p_nf                     => v_nf,
						  p_mnfst_serie            => v_bk_sanea(i).serie,
						  p_mnfst_ind_cont         => v_bk_sanea(i).mnfst_ind_cont_aux,
						  p_nr_qtde_inf            => v_nr_qtde_inf,
						  p_last_reg               => v_bk_sanea(i).last_reg_nf,
						  p_altera_nf              => v_dml.altera_nf);		
			
			-- <<FIM TRATAMENTO INF>>		

			IF v_bk_sanea(i).last_reg_nf = 'S' THEN	
				-- <<INICIO TRATAMENTO NF>>
				v_nr_qtde_inf := 0;
				-- <<FIM TRATAMENTO NF>>				
				-- DML NF
				IF v_dml.altera_nf AND v_chave_aux.rowid_nf IS NOT NULL THEN
					UPDATE openrisow.mestre_nftl_serv nf
					SET nf.TDOC_COD                       = v_nf.TDOC_COD             ,         
						nf.CATG_COD                       = v_nf.CATG_COD             ,         
						nf.CADG_COD                       = v_nf.CADG_COD             ,         
						nf.MNFST_IND_CONT                 = v_nf.MNFST_IND_CONT       ,         
						nf.MNFST_VAL_TOT                  = v_nf.MNFST_VAL_TOT        ,         
						nf.MNFST_VAL_DESC                 = v_nf.MNFST_VAL_DESC       ,         
						nf.MNFST_IND_CANC                 = v_nf.MNFST_IND_CANC       ,         
						nf.MNFST_DAT_VENC                 = v_nf.MNFST_DAT_VENC       ,         
						nf.MNFST_PER_REF                  = v_nf.MNFST_PER_REF        ,         
						nf.MNFST_AVISTA                   = v_nf.MNFST_AVISTA         ,         
						nf.NUM01                          = v_nf.NUM01                ,         
						nf.NUM02                          = v_nf.NUM02                ,         
						nf.NUM03                          = v_nf.NUM03                ,         
						nf.VAR01                          = v_nf.VAR01                ,         
						nf.VAR02                          = v_nf.VAR02                ,         
						nf.VAR03                          = v_nf.VAR03                ,         
						nf.VAR04                          = v_nf.VAR04                ,         
						nf.VAR05                          = v_nf.VAR05                ,         
						nf.MNFST_IND_CNV115               = v_nf.MNFST_IND_CNV115     ,         
						nf.CNPJ_CPF                       = v_nf.CNPJ_CPF             ,         
						nf.MNFST_VAL_BASICMS              = v_nf.MNFST_VAL_BASICMS    ,         
						nf.MNFST_VAL_ICMS                 = v_nf.MNFST_VAL_ICMS       ,         
						nf.MNFST_VAL_ISENTAS              = v_nf.MNFST_VAL_ISENTAS    ,         
						nf.MNFST_VAL_OUTRAS               = v_nf.MNFST_VAL_OUTRAS     ,         
						nf.MNFST_CODH_NF                  = v_nf.MNFST_CODH_NF        ,         
						nf.MNFST_CODH_REGNF               = v_nf.MNFST_CODH_REGNF     ,         
						nf.MNFST_CODH_REGCLI              = v_nf.MNFST_CODH_REGCLI    ,         
						nf.MNFST_REG_ESP                  = v_nf.MNFST_REG_ESP        ,         
						nf.MNFST_BAS_ICMS_ST              = v_nf.MNFST_BAS_ICMS_ST    ,         
						nf.MNFST_VAL_ICMS_ST              = v_nf.MNFST_VAL_ICMS_ST    ,         
						nf.MNFST_VAL_PIS                  = v_nf.MNFST_VAL_PIS        ,         
						nf.MNFST_VAL_COFINS               = v_nf.MNFST_VAL_COFINS     ,         
						nf.MNFST_VAL_DA                   = v_nf.MNFST_VAL_DA         ,         
						nf.MNFST_VAL_SER                  = v_nf.MNFST_VAL_SER        ,         
						nf.MNFST_VAL_TERC                 = v_nf.MNFST_VAL_TERC       ,         
						nf.CICD_COD_INF                   = v_nf.CICD_COD_INF         ,         
						nf.MNFST_TIP_ASSI                 = v_nf.MNFST_TIP_ASSI       ,         
						nf.MNFST_TIP_UTIL                 = v_nf.MNFST_TIP_UTIL       ,         
						nf.MNFST_GRP_TENS                 = v_nf.MNFST_GRP_TENS       ,         
						nf.MNFST_IND_EXTEMP               = v_nf.MNFST_IND_EXTEMP     ,         
						nf.MNFST_DAT_EXTEMP               = v_nf.MNFST_DAT_EXTEMP     ,         
						nf.MNFST_NUM_FIC                  = v_nf.MNFST_NUM_FIC        ,         
						nf.MNFST_DT_LT_ANT                = v_nf.MNFST_DT_LT_ANT      ,         
						nf.MNFST_DT_LT_ATU                = v_nf.MNFST_DT_LT_ATU      ,         
						nf.MNFST_NUM_FAT                  = v_nf.MNFST_NUM_FAT        ,         
						nf.MNFST_VL_TOT_FAT               = v_nf.MNFST_VL_TOT_FAT     ,         
						nf.MNFST_CHV_NFE                  = v_nf.MNFST_CHV_NFE        ,         
						nf.MNFST_DAT_AUT_NFE              = v_nf.MNFST_DAT_AUT_NFE    ,         
						nf.MNFST_VAL_DESC_PIS             = v_nf.MNFST_VAL_DESC_PIS   ,         
						nf.MNFST_VAL_DESC_COFINS          = v_nf.MNFST_VAL_DESC_COFINS
					WHERE nf.rowid = v_chave_aux.rowid_nf;
					:v_qtd_atu_nf   := :v_qtd_atu_nf + 1;
					v_dml.altera_nf := FALSE;
				END IF;					
			END IF; 

		END LOOP;    

	  END IF;       
	  ${COMMIT};	
	  EXIT WHEN c_sanea%NOTFOUND;	  
   END LOOP;        
   CLOSE c_sanea;   
	   
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

