-- CREATE OR REPLACE 
PROCEDURE prcts_regra_19(
		p_cli            IN OUT NOCOPY c_cli%ROWTYPE,
		p_f              IN c_f%ROWTYPE,
		p_inf            IN OUT NOCOPY c_inf%rowtype,
		p_nf             IN OUT NOCOPY c_nf%rowtype,
		p_sanea          IN OUT NOCOPY c_sanea%ROWTYPE,
		p_cp             IN c_cp%ROWTYPE,   
		p_st_t           IN OUT NOCOPY c_st%ROWTYPE,	
		p_nr_qtde_inf    IN OUT NOCOPY PLS_INTEGER)
AS
  -- MAP_2_REGRA_19_VAL_CONT_VAL_SERV
    v_item_nftl_serv   c_inf%rowtype;	
	
	PROCEDURE prc_tmp_insere_inf AS
	BEGIN
		INSERT INTO openrisow.item_nftl_serv 
		(
			emps_cod              , 
			fili_cod              , 
			cgc_cpf               , 
			ie                    , 
			uf                    , 
			tp_loc                , 
			localidade            , 
			tdoc_cod              , 
			infst_serie           , 
			infst_num             , 
			infst_dtemiss         , 
			catg_cod              , 
			cadg_cod              , 
			serv_cod              , 
			estb_cod              , 
			infst_dsc_compl       , 
			infst_val_cont        , 
			infst_val_serv        , 
			infst_val_desc        , 
			infst_aliq_icms       , 
			infst_base_icms       , 
			infst_val_icms        , 
			infst_isenta_icms     , 
			infst_outras_icms     , 
			infst_tribipi         ,
			infst_tribicms        , 
			infst_isenta_ipi      , 
			infst_outra_ipi       , 
			infst_outras_desp     , 
			infst_fiscal          , 
			infst_num_seq         , 
			infst_tel             , 
			infst_ind_canc        , 
			infst_proter          , 
			infst_cod_cont        , 
			cfop                  , 
			mdoc_cod              , 
			cod_prest             , 
			num01                 , 
			num02                 , 
			num03                 , 
			var01                 , 
			var02                 , 
			var03                 , 
			var04                 , 
			var05                 , 
			infst_ind_cnv115      , 
			infst_unid_medida     , 
			infst_quant_contr     , 
			infst_quant_prest     , 
			infst_codh_reg        , 
			esta_cod              , 
			infst_val_pis         , 
			infst_val_cofins      , 
			infst_bas_icms_st     , 
			infst_aliq_icms_st    , 
			infst_val_icms_st     , 
			infst_val_red         , 
			tpis_cod              , 
			tcof_cod              , 
			infst_bas_piscof      , 
			infst_aliq_pis        , 
			infst_aliq_cofins     , 
			infst_nat_rec         , 
			cscp_cod              , 
			infst_num_contr       , 
			infst_tip_isencao     , 
			infst_tar_aplic       , 
			infst_ind_desc        , 
			infst_num_fat         , 
			infst_qtd_fat         , 
			infst_mod_ativ        , 
			infst_hora_ativ       , 
			infst_id_equip        , 
			infst_mod_pgto        , 
			infst_num_nfe         , 
			infst_dtemiss_nfe     , 
			infst_val_cred_nfe    , 
			infst_cnpj_can_com    , 
			infst_val_desc_pis    , 
			infst_val_desc_cofins ,
			infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			infst_fcp_st	        -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	  
		)
		VALUES
		(
			v_item_nftl_serv.emps_cod              , 
			v_item_nftl_serv.fili_cod              , 
			v_item_nftl_serv.cgc_cpf               , 
			v_item_nftl_serv.ie                    , 
			v_item_nftl_serv.uf                    , 
			v_item_nftl_serv.tp_loc                , 
			v_item_nftl_serv.localidade            , 
			v_item_nftl_serv.tdoc_cod              , 
			v_item_nftl_serv.infst_serie           , 
			v_item_nftl_serv.infst_num             , 
			v_item_nftl_serv.infst_dtemiss         , 
			v_item_nftl_serv.catg_cod              , 
			v_item_nftl_serv.cadg_cod              , 
			v_item_nftl_serv.serv_cod              , 
			v_item_nftl_serv.estb_cod              , 
			v_item_nftl_serv.infst_dsc_compl       , 
			v_item_nftl_serv.infst_val_cont        , 
			v_item_nftl_serv.infst_val_serv        , 
			v_item_nftl_serv.infst_val_desc        , 
			v_item_nftl_serv.infst_aliq_icms       , 
			v_item_nftl_serv.infst_base_icms       , 
			v_item_nftl_serv.infst_val_icms        , 
			v_item_nftl_serv.infst_isenta_icms     , 
			v_item_nftl_serv.infst_outras_icms     , 
			v_item_nftl_serv.infst_tribipi         , 
			v_item_nftl_serv.infst_tribicms        , 
			v_item_nftl_serv.infst_isenta_ipi      , 
			v_item_nftl_serv.infst_outra_ipi       , 
			v_item_nftl_serv.infst_outras_desp     , 
			v_item_nftl_serv.infst_fiscal          , 
			v_item_nftl_serv.infst_num_seq         , 
			v_item_nftl_serv.infst_tel             , 
			v_item_nftl_serv.infst_ind_canc        , 
			v_item_nftl_serv.infst_proter          , 
			v_item_nftl_serv.infst_cod_cont        , 
			v_item_nftl_serv.cfop                  , 
			v_item_nftl_serv.mdoc_cod              , 
			v_item_nftl_serv.cod_prest             , 
			v_item_nftl_serv.num01                 , 
			v_item_nftl_serv.num02                 , 
			v_item_nftl_serv.num03                 , 
			v_item_nftl_serv.var01                 , 
			v_item_nftl_serv.var02                 , 
			v_item_nftl_serv.var03                 , 
			v_item_nftl_serv.var04                 , 
			v_item_nftl_serv.var05                 , 
			v_item_nftl_serv.infst_ind_cnv115      , 
			v_item_nftl_serv.infst_unid_medida     , 
			v_item_nftl_serv.infst_quant_contr     , 
			v_item_nftl_serv.infst_quant_prest     , 
			v_item_nftl_serv.infst_codh_reg        , 
			v_item_nftl_serv.esta_cod              , 
			v_item_nftl_serv.infst_val_pis         , 
			v_item_nftl_serv.infst_val_cofins      , 
			v_item_nftl_serv.infst_bas_icms_st     , 
			v_item_nftl_serv.infst_aliq_icms_st    , 
			v_item_nftl_serv.infst_val_icms_st     , 
			v_item_nftl_serv.infst_val_red         , 
			v_item_nftl_serv.tpis_cod              , 
			v_item_nftl_serv.tcof_cod              , 
			v_item_nftl_serv.infst_bas_piscof      , 
			v_item_nftl_serv.infst_aliq_pis        , 
			v_item_nftl_serv.infst_aliq_cofins     , 
			v_item_nftl_serv.infst_nat_rec         , 
			v_item_nftl_serv.cscp_cod              , 
			v_item_nftl_serv.infst_num_contr       , 
			v_item_nftl_serv.infst_tip_isencao     , 
			v_item_nftl_serv.infst_tar_aplic       , 
			v_item_nftl_serv.infst_ind_desc        , 
			v_item_nftl_serv.infst_num_fat         , 
			v_item_nftl_serv.infst_qtd_fat         , 
			v_item_nftl_serv.infst_mod_ativ        , 
			v_item_nftl_serv.infst_hora_ativ       , 
			v_item_nftl_serv.infst_id_equip        , 
			v_item_nftl_serv.infst_mod_pgto        , 
			v_item_nftl_serv.infst_num_nfe         , 
			v_item_nftl_serv.infst_dtemiss_nfe     , 
			v_item_nftl_serv.infst_val_cred_nfe    , 
			v_item_nftl_serv.infst_cnpj_can_com    , 
			v_item_nftl_serv.infst_val_desc_pis    , 
			v_item_nftl_serv.infst_val_desc_cofins ,
			v_item_nftl_serv.infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			v_item_nftl_serv.infst_fcp_st	         -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			) RETURNING ROWID INTO v_item_nftl_serv.rowid_inf;			
	END;
	
BEGIN
  IF p_sanea.serie IN ('1', 'UT')
	 AND ( p_inf.infst_dtemiss >= to_date('01/01/2015','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') ) 
  THEN
  
    IF (     NVL(p_inf.infst_val_cont,0)        = NVL(p_inf.infst_base_icms,0) 
	   AND NVL(p_inf.infst_val_cont,0)          < NVL(p_inf.infst_val_serv,0) 
	   AND NVL(p_inf.infst_isenta_icms,0)       = 0 
	   AND NVL(p_inf.infst_outras_icms,0)       = 0 
	   AND NVL(p_inf.infst_val_desc,0)          = 0 
	   AND NVL(p_inf.infst_base_icms,0)         <> 0 
			  )
	THEN
	
		IF p_inf.infst_num_seq_max > p_sanea.infst_num_seq_max THEN
			p_sanea.infst_num_seq_max := p_inf.infst_num_seq_max;
		ELSIF p_inf.infst_num_seq_max < p_sanea.infst_num_seq_max THEN
			p_inf.infst_num_seq_max := p_sanea.infst_num_seq_max;
		END IF;				
		
		IF v_nf.mnfst_ind_cont_aux > p_sanea.mnfst_ind_cont_aux THEN
			p_sanea.mnfst_ind_cont_aux := v_nf.mnfst_ind_cont_aux;
		ELSIF v_nf.mnfst_ind_cont_aux < p_sanea.mnfst_ind_cont_aux THEN
			v_nf.mnfst_ind_cont_aux := p_sanea.mnfst_ind_cont_aux;
		END IF;		
		
		p_inf.infst_num_seq_max            := NVL(p_inf.infst_num_seq_max,0)   + 1;		
		p_nf.mnfst_ind_cont_aux            := TRIM(TO_CHAR(TO_NUMBER(p_nf.mnfst_ind_cont_aux) + 1,'00000'));	
		IF TO_NUMBER(p_nf.mnfst_ind_cont_aux) >  p_inf.infst_num_seq_max THEN
			p_inf.infst_num_seq_max      := TO_NUMBER(p_nf.mnfst_ind_cont_aux);
		END IF;		
		p_sanea.mnfst_ind_cont_aux           := p_nf.mnfst_ind_cont_aux;
		p_sanea.infst_num_seq_max            := p_inf.infst_num_seq_max;
        v_item_nftl_serv                     := p_inf;		
		v_item_nftl_serv.infst_num_seq       := v_item_nftl_serv.infst_num_seq_max;
		v_item_nftl_serv.update_reg          := 0;	
		v_item_nftl_serv.infst_num_seq_aux   := NULL;
		v_item_nftl_serv.last_reg_nf         := 'N';
		v_item_nftl_serv.last_reg_cli        := 'N';
		
		-- v_item_nftl_serv.var05             := SUBSTR('prcts_regra_19i:' || v_item_nftl_serv.infst_val_desc || '|' || v_item_nftl_serv.infst_outras_icms || '|' || v_item_nftl_serv.infst_val_cont || '|' || v_item_nftl_serv.infst_val_serv || '|' || v_item_nftl_serv.infst_base_icms || '|' || v_item_nftl_serv.infst_val_icms || '|' || v_item_nftl_serv.estb_cod || '|' || v_item_nftl_serv.infst_aliq_icms || '|' || v_item_nftl_serv.infst_tribicms || '>>'|| v_item_nftl_serv.VAR05,1,150);
       	-- v_item_nftl_serv.infst_val_cont    := NVL(v_item_nftl_serv.infst_val_serv,0) - NVL(v_item_nftl_serv.infst_val_cont,0); --  dif_item_original := INFST_VAL_SERV - INFST_VAL_CONT
        -- v_item_nftl_serv.infst_val_serv    := v_item_nftl_serv.infst_val_cont;                                                 -- INFST_VAL_SERV      := dif_item_original
        -- v_item_nftl_serv.infst_outras_icms := v_item_nftl_serv.infst_val_cont;                                                 -- infst_outras_icms   := dif_item_original
        -- v_item_nftl_serv.infst_base_icms   := 0;
        -- v_item_nftl_serv.infst_val_icms    := 0;
        -- v_item_nftl_serv.infst_val_desc    := 0;
        -- v_item_nftl_serv.estb_cod          := '90';
        -- v_item_nftl_serv.infst_aliq_icms   := 0;
        -- v_item_nftl_serv.infst_tribicms    := 'P';	

		SELECT
			rowid as rowid_inf        ,
			inf.emps_cod              , 
			inf.fili_cod              , 
			inf.cgc_cpf               , 
			inf.ie                    , 
			inf.uf                    , 
			inf.tp_loc                , 
			inf.localidade            , 
			inf.tdoc_cod              , 
			inf.infst_serie           , 
			inf.infst_num             , 
			inf.infst_dtemiss         , 
			inf.catg_cod              , 
			inf.cadg_cod              , 
			inf.serv_cod              , 
			'90' AS estb_cod          , 
			inf.infst_dsc_compl       , 
			NVL(inf.infst_val_serv,0) - NVL(inf.infst_val_cont,0) AS infst_val_cont        , 
			NVL(inf.infst_val_serv,0) - NVL(inf.infst_val_cont,0) AS infst_val_serv        , 
			0 AS infst_val_desc       , 
			0 AS infst_aliq_icms       , 
			0 AS infst_base_icms      , 
			0 AS infst_val_icms       , 
			inf.infst_isenta_icms     , 
			NVL(inf.infst_val_serv,0) - NVL(inf.infst_val_cont,0) AS infst_outras_icms     , 
			inf.infst_tribipi         , 
			'P' AS infst_tribicms     , 
			inf.infst_isenta_ipi      , 
			inf.infst_outra_ipi       , 
			inf.infst_outras_desp     , 
			inf.infst_fiscal          , 
			v_item_nftl_serv.infst_num_seq_max AS infst_num_seq ,
			v_item_nftl_serv.infst_num_seq_max AS infst_num_seq_max    , 	
			inf.infst_tel             , 
			inf.infst_ind_canc        , 
			inf.infst_proter          , 
			inf.infst_cod_cont        , 
			inf.cfop                  , 
			inf.mdoc_cod              , 
			inf.cod_prest             , 
			inf.num01                 , 
			inf.num02                 , 
			inf.num03                 , 
			inf.var01                 , 
			inf.var02                 , 
			inf.var03                 , 
			inf.var04                 , 
			SUBSTR('prcts_regra_19i:' || inf.CFOP || '|' || inf.infst_outras_icms || '|' || inf.infst_val_cont || '|' || inf.infst_val_serv || '|' || inf.infst_base_icms || '|' || inf.infst_val_icms || '|' || inf.estb_cod || '|' || inf.infst_aliq_icms || '|' || inf.infst_tribicms || '>>'|| inf.var05,1,150) AS var05   , 
			inf.infst_ind_cnv115      , 
			inf.infst_unid_medida     , 
			inf.infst_quant_contr     , 
			inf.infst_quant_prest     , 
			inf.infst_codh_reg        , 
			inf.esta_cod              , 
			inf.infst_val_pis         , 
			inf.infst_val_cofins      , 
			inf.infst_bas_icms_st     , 
			inf.infst_aliq_icms_st    , 
			inf.infst_val_icms_st     , 
			inf.infst_val_red         , 
			inf.tpis_cod              , 
			inf.tcof_cod              , 
			inf.infst_bas_piscof      , 
			inf.infst_aliq_pis        , 
			inf.infst_aliq_cofins     , 
			inf.infst_nat_rec         , 
			inf.cscp_cod              , 
			inf.infst_num_contr       , 
			inf.infst_tip_isencao     , 
			inf.infst_tar_aplic       , 
			inf.infst_ind_desc        , 
			inf.infst_num_fat         , 
			inf.infst_qtd_fat         , 
			inf.infst_mod_ativ        , 
			inf.infst_hora_ativ       , 
			inf.infst_id_equip        , 
			inf.infst_mod_pgto        , 
			inf.infst_num_nfe         , 
			inf.infst_dtemiss_nfe     , 
			inf.infst_val_cred_nfe    , 
			inf.infst_cnpj_can_com    , 
			inf.infst_val_desc_pis    , 
			inf.infst_val_desc_cofins ,
			inf.infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			inf.infst_fcp_st	        -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
		INTO		
			v_item_nftl_serv.rowid_inf             ,
			v_item_nftl_serv.emps_cod              , 
			v_item_nftl_serv.fili_cod              , 
			v_item_nftl_serv.cgc_cpf               , 
			v_item_nftl_serv.ie                    , 
			v_item_nftl_serv.uf                    , 
			v_item_nftl_serv.tp_loc                , 
			v_item_nftl_serv.localidade            , 
			v_item_nftl_serv.tdoc_cod              , 
			v_item_nftl_serv.infst_serie           , 
			v_item_nftl_serv.infst_num             , 
			v_item_nftl_serv.infst_dtemiss         , 
			v_item_nftl_serv.catg_cod              , 
			v_item_nftl_serv.cadg_cod              , 
			v_item_nftl_serv.serv_cod              , 
			v_item_nftl_serv.estb_cod              , 
			v_item_nftl_serv.infst_dsc_compl       , 
			v_item_nftl_serv.infst_val_cont        , 
			v_item_nftl_serv.infst_val_serv        , 
			v_item_nftl_serv.infst_val_desc        , 
			v_item_nftl_serv.infst_aliq_icms       , 
			v_item_nftl_serv.infst_base_icms       , 
			v_item_nftl_serv.infst_val_icms        , 
			v_item_nftl_serv.infst_isenta_icms     , 
			v_item_nftl_serv.infst_outras_icms     , 
			v_item_nftl_serv.infst_tribipi         , 
			v_item_nftl_serv.infst_tribicms        , 
			v_item_nftl_serv.infst_isenta_ipi      , 
			v_item_nftl_serv.infst_outra_ipi       , 
			v_item_nftl_serv.infst_outras_desp     , 
			v_item_nftl_serv.infst_fiscal          , 
			v_item_nftl_serv.infst_num_seq         , 
			v_item_nftl_serv.infst_num_seq_max     ,
			v_item_nftl_serv.infst_tel             , 
			v_item_nftl_serv.infst_ind_canc        , 
			v_item_nftl_serv.infst_proter          , 
			v_item_nftl_serv.infst_cod_cont        , 
			v_item_nftl_serv.cfop                  , 
			v_item_nftl_serv.mdoc_cod              , 
			v_item_nftl_serv.cod_prest             , 
			v_item_nftl_serv.num01                 , 
			v_item_nftl_serv.num02                 , 
			v_item_nftl_serv.num03                 , 
			v_item_nftl_serv.var01                 , 
			v_item_nftl_serv.var02                 , 
			v_item_nftl_serv.var03                 , 
			v_item_nftl_serv.var04                 , 
			v_item_nftl_serv.var05                 , 
			v_item_nftl_serv.infst_ind_cnv115      , 
			v_item_nftl_serv.infst_unid_medida     , 
			v_item_nftl_serv.infst_quant_contr     , 
			v_item_nftl_serv.infst_quant_prest     , 
			v_item_nftl_serv.infst_codh_reg        , 
			v_item_nftl_serv.esta_cod              , 
			v_item_nftl_serv.infst_val_pis         , 
			v_item_nftl_serv.infst_val_cofins      , 
			v_item_nftl_serv.infst_bas_icms_st     , 
			v_item_nftl_serv.infst_aliq_icms_st    , 
			v_item_nftl_serv.infst_val_icms_st     , 
			v_item_nftl_serv.infst_val_red         , 
			v_item_nftl_serv.tpis_cod              , 
			v_item_nftl_serv.tcof_cod              , 
			v_item_nftl_serv.infst_bas_piscof      , 
			v_item_nftl_serv.infst_aliq_pis        , 
			v_item_nftl_serv.infst_aliq_cofins     , 
			v_item_nftl_serv.infst_nat_rec         , 
			v_item_nftl_serv.cscp_cod              , 
			v_item_nftl_serv.infst_num_contr       , 
			v_item_nftl_serv.infst_tip_isencao     , 
			v_item_nftl_serv.infst_tar_aplic       , 
			v_item_nftl_serv.infst_ind_desc        , 
			v_item_nftl_serv.infst_num_fat         , 
			v_item_nftl_serv.infst_qtd_fat         , 
			v_item_nftl_serv.infst_mod_ativ        , 
			v_item_nftl_serv.infst_hora_ativ       , 
			v_item_nftl_serv.infst_id_equip        , 
			v_item_nftl_serv.infst_mod_pgto        , 
			v_item_nftl_serv.infst_num_nfe         , 
			v_item_nftl_serv.infst_dtemiss_nfe     , 
			v_item_nftl_serv.infst_val_cred_nfe    , 
			v_item_nftl_serv.infst_cnpj_can_com    , 
			v_item_nftl_serv.infst_val_desc_pis    , 
			v_item_nftl_serv.infst_val_desc_cofins ,
			v_item_nftl_serv.infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			v_item_nftl_serv.infst_fcp_st	         -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185			
		FROM  openrisow.item_nftl_serv inf	
		WHERE 	inf.ROWID = p_inf.rowid_inf;

		BEGIN
		
			prc_tmp_insere_inf;
			
		EXCEPTION
		WHEN DUP_VAL_ON_INDEX THEN
				
				SELECT MAX(inf1.infst_num_seq) + 1 
				   INTO v_item_nftl_serv.infst_num_seq_max
				FROM openrisow.item_nftl_serv inf1
				WHERE inf1.emps_cod     = p_inf.emps_cod 
				AND inf1.fili_cod       = p_inf.fili_cod 
				AND inf1.infst_serie    = p_inf.infst_serie 
				AND inf1.infst_dtemiss  = p_inf.infst_dtemiss 
				AND inf1.infst_num      = p_inf.infst_num; 	
			  
			    v_item_nftl_serv.infst_num_seq := v_item_nftl_serv.infst_num_seq_max;
				p_sanea.infst_num_seq_max      := v_item_nftl_serv.infst_num_seq_max;	
				p_inf.infst_num_seq_max        := p_sanea.infst_num_seq_max;
		
				prc_tmp_insere_inf;
				
		END;					
		:v_qtd_ins_inf                     := :v_qtd_ins_inf + 1;
		-- <<INICIO TRATAMENTO INF>>
		prcts_tratar_inf(p_cli1            => p_cli,
						 p_f1              => p_f,
						 p_inf1            => v_item_nftl_serv,
						 p_nf1             => p_nf,
						 p_sanea1          => p_sanea,
						 p_cp1             => p_cp,   
						 p_st_t1           => p_st_t,	
						 p_nr_qtde_inf1    => p_nr_qtde_inf);
		-- <<FIM TRATAMENTO INF>>
		-- Altera o registro original
        p_nr_qtde_inf                         := p_nr_qtde_inf + 1;
		p_inf.update_reg        			  := 1;
	    p_inf.var05                           := SUBSTR('prcts_regra_19u:' || p_inf.infst_outras_icms || '|' || p_inf.infst_val_cont || '|' || p_inf.infst_val_serv || '>>'|| p_inf.var05,1,150);
        p_inf.infst_val_serv                  := p_inf.infst_val_cont;	
		v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
	  									p_nm_var01      =>  p_inf.emps_cod,
	  									p_nm_var02      =>  p_inf.fili_cod,
	  									p_nm_var03      =>  p_inf.infst_serie,
	  									p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
	  									p_nm_var05      =>  '|R2015_19|',
	  									p_nr_var02      =>  1);			
    END IF;
  END IF;
END;
--/