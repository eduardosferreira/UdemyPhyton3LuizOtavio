-- CREATE OR REPLACE 
PROCEDURE prcts_regra_42B(
		p_cli            IN OUT NOCOPY c_cli%ROWTYPE,
		p_f              IN c_f%ROWTYPE,
		p_inf            IN OUT NOCOPY c_inf%rowtype,
		p_nf             IN OUT NOCOPY c_nf%rowtype,
		p_sanea          IN OUT NOCOPY c_sanea%ROWTYPE,
		p_cp             IN c_cp%ROWTYPE,   
		p_st_t           IN OUT NOCOPY c_st%ROWTYPE,	
		p_nr_qtde_inf    IN OUT NOCOPY PLS_INTEGER)
AS
    v_item_nftl_serv   c_inf%rowtype;	
	v_fl_exists 	   PLS_INTEGER := 0;
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
			v_item_nftl_serv.infst_fcp_st	        -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			) RETURNING ROWID INTO v_item_nftl_serv.rowid_inf;			
	END;
	
BEGIN

    IF                  p_inf.ESTB_COD       = '20'
		AND NVL(p_inf.INFST_VAL_CONT,0)      > 0
		AND NVL(p_inf.INFST_VAL_SERV,0)      > 0 
		AND NVL(p_inf.INFST_VAL_DESC,0)      = 0
		AND NVL(p_inf.INFST_BASE_ICMS,0)     > 0
		AND NVL(p_inf.INFST_VAL_ICMS,0)      > 0
		AND NVL(p_inf.INFST_ISENTA_ICMS,0)   > 0 
		AND NVL(p_inf.INFST_OUTRAS_ICMS,0)   = 0  
		AND p_inf.infst_num_seq_max          > 1
    THEN  	
	    IF 	NVL(v_bk_c_inf.COUNT,0) <= 0 THEN 
			OPEN c_inf(p_emps_cod         => p_inf.emps_cod,
					   p_fili_cod         => p_inf.fili_cod,
					   p_infst_dtemiss    => p_inf.infst_dtemiss,
					   p_infst_serie      => p_inf.infst_serie,
					   p_infst_num        => p_inf.infst_num);  
			FETCH c_inf BULK COLLECT INTO v_bk_c_inf;
			CLOSE c_inf;
		END IF;	
		IF 	NVL(v_bk_c_inf.COUNT,0) <= 0 THEN 
			RETURN;
		END IF;
		FOR i IN v_bk_c_inf.FIRST .. v_bk_c_inf.LAST 
		LOOP	
			IF    v_bk_c_inf(i).estb_cod                   = '90'
			  AND v_bk_c_inf(i).infst_tribicms             = 'P'			  
			  AND NVL(v_bk_c_inf(i).infst_val_cont,0)       < 0
			  AND NVL(v_bk_c_inf(i).infst_val_serv,0)       < 0
			  AND NVL(v_bk_c_inf(i).infst_outras_icms,0)    < 0
			  AND NVL(v_bk_c_inf(i).infst_val_desc,0)       = 0
			  AND NVL(v_bk_c_inf(i).infst_aliq_icms,0)      = 0
			  AND NVL(v_bk_c_inf(i).infst_base_icms,0)      = 0
			  AND NVL(v_bk_c_inf(i).infst_val_icms,0)       = 0
			  AND NVL(v_bk_c_inf(i).infst_isenta_icms,0)    = 0			  
			THEN
			    IF
				      NVL(p_inf.infst_isenta_icms,0) >= ABS(v_bk_c_inf(i).infst_outras_icms) 
                   AND p_inf.infst_num_seq = v_bk_c_inf(i).infst_num_seq - 1
                THEN  			  
				   IF v_bk_c_inf(i).serv_cod = p_inf.serv_cod
				   OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p_inf.SERV_COD,REPLACE(p_inf.INFST_SERIE,' ','')||'C08',''),REPLACE(p_inf.INFST_SERIE,' ','')||'C09',''),REPLACE(p_inf.INFST_SERIE,' ','')||'ZP',''),REPLACE(p_inf.INFST_SERIE,' ','')||'ZN',''),REPLACE(p_inf.INFST_SERIE,' ','')||'C',''),REPLACE(p_inf.INFST_SERIE,' ','')||'L','') =  
                      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(v_bk_c_inf(i).SERV_COD,REPLACE(v_bk_c_inf(i).INFST_SERIE,' ','')||'C08',''),REPLACE(v_bk_c_inf(i).INFST_SERIE,' ','')||'C09',''),REPLACE(v_bk_c_inf(i).INFST_SERIE,' ','')||'ZP',''),REPLACE(v_bk_c_inf(i).INFST_SERIE,' ','')||'ZN',''),REPLACE(v_bk_c_inf(i).INFST_SERIE,' ','')||'C',''),REPLACE(v_bk_c_inf(i).INFST_SERIE,' ','')||'C',''),REPLACE(v_bk_c_inf(i).INFST_SERIE,' ','')||'L','')
				   --OR fccts_retira_caracter(v_bk_c_inf(i).serv_cod) = fccts_retira_caracter(p_inf.serv_cod)
				   THEN
						v_item_nftl_serv                     := v_bk_c_inf(i);	
				        v_fl_exists                          := v_fl_exists + 1;	
				        EXIT;
				   END IF;
                END IF;			  
			END IF;
		END LOOP;
		IF v_fl_exists <= 0 THEN
			RETURN;
		END IF;
		
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
        
        v_item_nftl_serv.infst_num_seq_max   := p_inf.infst_num_seq_max;		
		v_item_nftl_serv.infst_num_seq       := v_item_nftl_serv.infst_num_seq_max;
		v_item_nftl_serv.update_reg          := 0;	
		v_item_nftl_serv.infst_num_seq_aux   := NULL;
		v_item_nftl_serv.last_reg_nf         := 'N';
		v_item_nftl_serv.last_reg_cli        := 'N';	
		
		v_item_nftl_serv.var05               :=  SUBSTR('prcts_regra_42Bi:' || v_item_nftl_serv.infst_val_desc || '|' || v_item_nftl_serv.infst_outras_icms || '|' || v_item_nftl_serv.infst_val_cont || '|' || v_item_nftl_serv.infst_val_serv || '|' || v_item_nftl_serv.infst_base_icms || '|' || v_item_nftl_serv.infst_val_icms || '|' || v_item_nftl_serv.estb_cod || '|' || v_item_nftl_serv.infst_aliq_icms || '|' || v_item_nftl_serv.infst_tribicms || '>>'|| v_item_nftl_serv.VAR05,1,150);
       	v_item_nftl_serv.INFST_VAL_CONT      :=  ABS(NVL(v_item_nftl_serv.INFST_VAL_CONT,0));
		v_item_nftl_serv.INFST_VAL_SERV      :=  ABS(NVL(v_item_nftl_serv.INFST_VAL_SERV,0));		
		v_item_nftl_serv.INFST_OUTRAS_ICMS   :=  ABS(NVL(v_item_nftl_serv.INFST_OUTRAS_ICMS,0));

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
	    p_inf.VAR05                           := SUBSTR('prcts_regra_42Bu:' || p_inf.INFST_ISENTA_ICMS || '|' || p_inf.infst_val_cont || '|' || p_inf.INFST_VAL_SERV|| '|' || p_inf.ESTB_COD|| '|' || p_inf.INFST_TRIBICMS || '|' || p_inf.INFST_VAL_RED || '>>'|| p_inf.VAR05,1,150);
	    p_inf.INFST_VAL_CONT                  := NVL(p_inf.INFST_VAL_CONT,0)    - ABS(v_item_nftl_serv.INFST_OUTRAS_ICMS);
	    p_inf.INFST_VAL_SERV                  := NVL(p_inf.INFST_VAL_SERV,0)    - ABS(v_item_nftl_serv.INFST_OUTRAS_ICMS);
	    p_inf.INFST_ISENTA_ICMS               := NVL(p_inf.INFST_ISENTA_ICMS,0) - ABS(v_item_nftl_serv.INFST_OUTRAS_ICMS);
		
		IF NVL(p_inf.infst_isenta_icms,0) <> 0 THEN  
           p_inf.estb_cod := '20';
		ELSIF NVL(p_inf.infst_isenta_icms,0) = 0 THEN  
           p_inf.estb_cod := '00';
		END IF;   
		
		v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
													 p_nm_var01      =>  p_inf.emps_cod,
													 p_nm_var02      =>  p_inf.fili_cod,
													 p_nm_var03      =>  p_inf.infst_serie,
													 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
													 p_nm_var05      =>  '|R2016_42B|',
													 p_nr_var02      =>  1);	
  END IF;
  
END;
--/