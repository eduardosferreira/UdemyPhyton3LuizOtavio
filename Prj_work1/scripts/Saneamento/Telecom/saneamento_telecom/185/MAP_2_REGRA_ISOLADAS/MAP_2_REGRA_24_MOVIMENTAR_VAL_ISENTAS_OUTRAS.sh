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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_24_MOVIMENTAR_VAL_ISENTAS_OUTRAS'
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
PROMPT MAP_2_REGRA_24_MOVIMENTAR_VAL_ISENTAS_OUTRAS
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT
DECLARE
   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
  
   CURSOR c_tmp_st_04
	 IS
    SELECT * FROM gfcadastro.tmp_st_04;   
   TYPE t_tab_tmp_st_04 IS TABLE OF c_tmp_st_04%ROWTYPE INDEX BY VARCHAR2(500);
   v_tab_tmp_st_04 t_tab_tmp_st_04;  
  
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
		inf.rowid AS rowid_inf,     
		inf.*   
	FROM openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf,     
	     openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf   
	WHERE ${FILTRO} 
		AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
	AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
        -- AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2016','DD/MM/YYYY')
        AND inf.emps_cod      = nf.emps_cod   
		AND inf.fili_cod      = nf.fili_cod   
		AND inf.infst_serie   = nf.mnfst_serie   
		AND inf.infst_num     = nf.mnfst_num   
		AND inf.infst_dtemiss = nf.mnfst_dtemiss   
	ORDER BY -- CLI , LAST_REG_CLI , 
	         ROWID_NF
		   , LAST_REG_NF;	
   TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_sanea t_sanea;
   v_sanea    c_sanea%ROWTYPE;

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
PROCEDURE prcts_tab_tmp_st_04 
AS 
BEGIN
FOR i IN c_tmp_st_04
LOOP
	v_tab_tmp_st_04(UPPER(TRIM(TRANSLATE(TRIM(i.servtl_cod), '" ', '"')))) := i;
END LOOP;   
END;   
PROCEDURE prcts_regra_24(
    p_inf            IN OUT openrisow.item_nftl_serv%rowtype,
    p_mnfst_serie    IN openrisow.mestre_nftl_serv.mnfst_serie%type,
    p_altera_inf     IN OUT BOOLEAN)
AS
	v_serv_cod openrisow.item_nftl_serv.serv_cod%type; 
	v_exists PLS_INTEGER := 0;
BEGIN
	-- Regra 
	IF  ((NVL(p_inf.INFST_OUTRAS_ICMS,0)      <> 0 
	   or NVL(p_inf.INFST_ISENTA_ICMS,0)      <> 0))
		AND NVL(p_inf.INFST_VAL_CONT,0)        = NVL(p_inf.INFST_VAL_SERV,0)
		AND NVL(p_inf.INFST_BASE_ICMS,0)       = 0
		AND NVL(p_inf.INFST_VAL_ICMS,0)        = 0
		AND NVL(p_inf.INFST_VAL_DESC,0)        = 0
		AND (  NVL(p_inf.CFOP,'_')            <> '0000' 
			OR NVL(p_inf.INFST_TRIBICMS,'_')  <> 'P' 
			OR NVL(p_inf.ESTB_COD,'_')        <> '90' 
			OR NVL(p_inf.INFST_ISENTA_ICMS,0) <> 0 
			OR NVL(p_inf.INFST_ALIQ_ICMS,0)   <> 0)
	THEN
	
		v_serv_cod               := UPPER(TRIM(TRANSLATE(TRIM(p_inf.serv_cod), '" ', '"')));
		v_serv_cod               := trim(substr(replace(replace(replace(replace(v_serv_cod,p_mnfst_serie||'ZP',''),p_mnfst_serie||'ZN',''),p_mnfst_serie||'Z',''),p_mnfst_serie,''),1,60));          
		
		DECLARE
		  v_idx VARCHAR2(500) := NULL;
		BEGIN
		  v_idx        := v_tab_tmp_st_04.first;
		  IF v_idx IS NULL THEN
			prcts_tab_tmp_st_04;
			v_idx        := v_tab_tmp_st_04.first;
		  END IF;
		  WHILE (v_idx IS NOT NULL)
		  LOOP
			IF UPPER(TRIM(TRANSLATE(TRIM(v_tab_tmp_st_04(v_idx).servtl_cod), '" ', '"'))) = v_serv_cod
			   OR UPPER(TRIM(TRANSLATE(TRIM(v_tab_tmp_st_04(v_idx).servtl_cod), '" ', '"'))) = UPPER(TRIM(TRANSLATE(TRIM(p_inf.serv_cod), '" ', '"')))
			THEN 
			   v_exists := 1;	
			   v_idx := NULL;
			ELSE
			   v_idx := v_tab_tmp_st_04.next(v_idx);
			END IF;
		  END LOOP;
		EXCEPTION
		  WHEN OTHERS THEN
			  v_exists := 0;  	  
		END;   
		
		IF v_exists = 1 THEN
			p_altera_inf             := TRUE;
			p_inf.var05              := substr('r2016_24u<1>:' || p_inf.INFST_VAL_RED || p_inf.infst_isenta_icms || '|' || p_inf.infst_outras_icms || '|' || p_inf.estb_cod || '|' || p_inf.infst_tribicms || '|' || p_inf.infst_aliq_icms || '|' || p_inf.cfop || '>>' ||p_inf.VAR05,1,150) ;	 			
			p_inf.INFST_OUTRAS_ICMS  :=  NVL(p_inf.INFST_OUTRAS_ICMS,0) + NVL(p_inf.INFST_ISENTA_ICMS,0);
			p_inf.INFST_ISENTA_ICMS  := 0;
			p_inf.INFST_VAL_RED      := 0;
			p_inf.INFST_ALIQ_ICMS    := 0;
			p_inf.ESTB_COD           := '90';
			p_inf.INFST_TRIBICMS     := 'P';
			p_inf.CFOP               := '0000';
		END IF;
		
	END IF;
	

END;
-- /
 
  
BEGIN
   
   -- Inicializacao
   prc_tempo('Inicializacao');
   v_dml.altera_inf :=  FALSE;
   
   prc_tempo('SANEA');
   OPEN c_sanea;
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
			
			-- IF   INSTR(v_cp.REGRAS,',' || 'R2015_38' || ',') > 0  OR INSTR(v_cp.REGRAS,',' || 'ALL' || ',') > 0 THEN
			prcts_regra_24(p_inf            => v_inf,
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

