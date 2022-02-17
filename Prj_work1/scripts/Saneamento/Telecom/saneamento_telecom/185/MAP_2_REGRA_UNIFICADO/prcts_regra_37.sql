-- CREATE OR REPLACE 
PROCEDURE prcts_regra_37(p_inf         IN OUT NOCOPY c_inf%rowtype, p_nf         IN OUT NOCOPY c_nf%rowtype, pCOMMIT IN VARCHAR2 := 'COMMIT')
AS
	v_inf_37 	          c_inf%rowtype;
	v_idx_37              c_37_i%TYPE;
	v_nro_idx_37          PLS_INTEGER :=0;	
BEGIN
   -- IF ( p_inf.infst_dtemiss >= to_date('01/01/2016','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') ) 	THEN
		v_idx_37      := p_inf.emps_cod || '|' || 
						 p_inf.fili_cod || '|' ||
						 TO_CHAR(p_inf.infst_dtemiss,'YYYYMMDD') || '|' || 
						 p_inf.infst_num || '|' || 
						 p_inf.serie || '|' || 
						 TRIM(p_inf.serv_cod) || '|' || 
						 TRIM(p_inf.estb_cod);	 
		IF  v_bk_c_37_t.exists(v_idx_37) THEN
			v_inf_37                     := v_bk_c_37_t(v_idx_37);
			v_inf_37.infst_val_cont      := NVL(v_inf_37.infst_val_cont,0)    + NVL(p_inf.infst_val_cont,0);
			v_inf_37.infst_isenta_icms   := NVL(v_inf_37.infst_isenta_icms,0) + NVL(p_inf.infst_isenta_icms,0);
			v_inf_37.infst_outras_icms   := NVL(v_inf_37.infst_outras_icms,0) + NVL(p_inf.infst_outras_icms,0);
			v_inf_37.infst_val_serv      := NVL(v_inf_37.infst_val_serv,0)    + NVL(p_inf.infst_val_serv,0);
			v_inf_37.infst_val_icms      := NVL(v_inf_37.infst_val_icms,0)    + NVL(p_inf.infst_val_icms,0);
			v_inf_37.infst_base_icms     := NVL(v_inf_37.infst_base_icms,0)   + NVL(p_inf.infst_base_icms,0);
			v_inf_37.infst_val_desc      := NVL(v_inf_37.infst_val_desc,0)    + NVL(p_inf.infst_val_desc,0);
			IF UPPER(TRIM(pCOMMIT)) = 'COMMIT' THEN
				DELETE FROM openrisow.item_nftl_serv inf WHERE inf.rowid  = p_inf.rowid_inf;	
			END IF;
		ELSE
			v_inf_37 := p_inf;
			IF UPPER(TRIM(pCOMMIT)) = 'COMMIT' THEN
				UPDATE openrisow.item_nftl_serv inf SET infst_num_seq = infst_num_seq + 10000 WHERE inf.rowid  = p_inf.rowid_inf;
			END IF;
		END IF;
		p_inf.rowid_inf              := NULL;
		p_inf.update_reg             := 0;
		v_bk_c_37_t(v_idx_37)        := v_inf_37;
		IF p_inf.last_reg_nf = 'S'
		THEN
			v_idx_37        := NULL;
			v_nro_idx_37    := 0;
			v_idx_37        := v_bk_c_37_t.first;
			WHILE (v_idx_37 IS NOT NULL)
			LOOP
			  IF v_bk_c_37_t.exists(v_idx_37) THEN
				IF v_bk_c_37_t(v_idx_37).rowid_inf IS NOT NULL AND UPPER(TRIM(pCOMMIT)) = 'COMMIT' THEN
					DELETE FROM openrisow.item_nftl_serv inf 
					WHERE inf.rowid      != v_bk_c_37_t(v_idx_37).rowid_inf
					AND inf.emps_cod      = v_bk_c_37_t(v_idx_37).emps_cod
					AND inf.fili_cod      = v_bk_c_37_t(v_idx_37).fili_cod
					AND inf.infst_serie   = v_bk_c_37_t(v_idx_37).infst_serie
					AND inf.infst_num     = v_bk_c_37_t(v_idx_37).infst_num
					AND inf.infst_dtemiss = v_bk_c_37_t(v_idx_37).infst_dtemiss
					AND inf.serv_cod      = v_bk_c_37_t(v_idx_37).serv_cod
					AND inf.estb_cod      = v_bk_c_37_t(v_idx_37).estb_cod;
					v_nro_idx_37         := v_nro_idx_37 + 1;
					v_bk_c_37_t(v_idx_37).infst_num_seq := v_nro_idx_37;
					v_bk_inf(nvl(v_bk_inf.COUNT,0)+1) := v_bk_c_37_t(v_idx_37);
					:v_qtd_atu_inf   := :v_qtd_atu_inf + 1;		
				END IF;
			  END IF;
			  v_idx_37 := v_bk_c_37_t.next(v_idx_37);
			END LOOP;
			v_bk_c_37_t.DELETE;		
			IF v_nro_idx_37 > 0 THEN
				p_nf.mnfst_ind_cont         := TRIM(TO_CHAR(v_nro_idx_37,'00000'));
				p_nf.update_reg             := 1;
				v_nr_limite_nf_37           := v_nr_limite_nf_37 + 1;
				v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
									                         p_nm_var01      =>  p_inf.emps_cod,
									                         p_nm_var02      =>  p_inf.fili_cod,
									                         p_nm_var03      =>  p_inf.infst_serie,
									                         p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
									                         p_nm_var05      =>  '|R2016_37|',
									                         p_nr_var02      =>  1);					
				IF v_nr_limite_nf_37 > 20 AND UPPER(TRIM(pCOMMIT)) = 'COMMIT' THEN
					prcts_stop('NR. MAX SUPORTADO PARA REGRA 37 >> ' || v_nr_limite_nf_37); 
				END IF;	
			END IF;				
		END IF;		
	-- END IF;	
	
END;
--/