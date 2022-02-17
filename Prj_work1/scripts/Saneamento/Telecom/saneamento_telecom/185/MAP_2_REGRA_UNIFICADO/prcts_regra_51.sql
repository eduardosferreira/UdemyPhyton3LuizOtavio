-- CREATE OR REPLACE 
PROCEDURE prcts_regra_51(
		p_cli            IN OUT NOCOPY c_cli%ROWTYPE,
		p_f              IN c_f%ROWTYPE,
		p_inf            IN OUT NOCOPY c_inf%rowtype,
		p_nf             IN OUT NOCOPY c_nf%rowtype,
		p_sanea          IN OUT NOCOPY c_sanea%ROWTYPE,
		p_cp             IN c_cp%ROWTYPE,   
		p_st_t           IN OUT NOCOPY c_st%ROWTYPE,	
		p_nr_qtde_inf    IN OUT NOCOPY PLS_INTEGER)
AS
	    v_inf_insert c_inf%rowtype;
-- infst_serie in('C')
-- INFST_ISENTA_ICMS = 0
-- INFST_BASE_ICMS = 0
-- INFST_VAL_ICMS = 0
-- INFST_VAL_DESC > 0
-- INFST_VAL_DESC = INFST_OUTRAS_ICMS
-- INFST_VAL_SERV = INFST_OUTRAS_ICMS
-- INFST_VAL_CONT = INFST_OUTRAS_ICMS
-- Então:
-- INFST_VAL_DESC := 0
-- Cria item baseado no original onde 
-- INFST_VAL_DESC := INFST_OUTRAS_ICMS * (-1)
-- INFST_VAL_SERV := INFST_OUTRAS_ICMS * (-1)
-- INFST_VAL_CONT := INFST_OUTRAS_ICMS * (-1)
-- INFST_VAL_DESC := 0
BEGIN
  IF    p_inf.rowid_inf IS NOT NULL 
    AND p_inf.serie IN ('C')
  THEN 
    IF NVL(p_inf.infst_isenta_icms,0)  = 0 
	  AND NVL(p_inf.infst_base_icms,0) = 0 
	  AND NVL(p_inf.infst_val_icms,0) = 0 
	  AND NVL(p_inf.infst_val_desc,0) > 0 
	  AND NVL(p_inf.infst_val_desc,0) = NVL(p_inf.infst_outras_icms,0) 
	  AND NVL(p_inf.infst_val_serv,0) = NVL(p_inf.infst_outras_icms,0) 
	  AND NVL(p_inf.infst_val_cont,0) = NVL(p_inf.infst_outras_icms,0) 
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
		
		prc_tmp_inf_find_insert(p_inf_find => p_inf, p_inf_insert => v_inf_insert);

		v_inf_insert.var05             := SUBSTR('prcts_regra_51i:' || v_inf_insert.INFST_OUTRAS_ICMS || '|' || v_inf_insert.infst_val_cont || '|' || v_inf_insert.infst_val_serv || '|' || v_inf_insert.INFST_VAL_DESC  || '>>'|| v_inf_insert.var05,1,150);
		v_inf_insert.INFST_VAL_DESC    := NVL(v_inf_insert.INFST_OUTRAS_ICMS,0) * (-1);
		v_inf_insert.INFST_VAL_SERV    := NVL(v_inf_insert.INFST_OUTRAS_ICMS,0) * (-1);
		v_inf_insert.INFST_VAL_CONT    := NVL(v_inf_insert.INFST_OUTRAS_ICMS,0) * (-1);
		v_inf_insert.INFST_OUTRAS_ICMS := NVL(v_inf_insert.INFST_OUTRAS_ICMS,0) * (-1);
		v_inf_insert.INFST_VAL_DESC    := 0;
		
		BEGIN
		
			prc_tmp_inf_insert(p_inf_insert => v_inf_insert);
			
		EXCEPTION
		WHEN DUP_VAL_ON_INDEX THEN
				
				SELECT MAX(inf1.infst_num_seq) + 1 
				   INTO v_inf_insert.infst_num_seq_max
				FROM openrisow.item_nftl_serv inf1
				WHERE inf1.emps_cod     = p_inf.emps_cod 
				AND inf1.fili_cod       = p_inf.fili_cod 
				AND inf1.infst_serie    = p_inf.infst_serie 
				AND inf1.infst_dtemiss  = p_inf.infst_dtemiss 
				AND inf1.infst_num      = p_inf.infst_num; 	
			  
				v_inf_insert.infst_num_seq := v_inf_insert.infst_num_seq_max;
				p_sanea.infst_num_seq_max      := v_inf_insert.infst_num_seq_max;	
				p_inf.infst_num_seq_max        := p_sanea.infst_num_seq_max;
		
				prc_tmp_inf_insert(p_inf_insert => v_inf_insert);
				
		END;	
		
		:v_qtd_ins_inf                     := :v_qtd_ins_inf + 1;	
		-- <<INICIO TRATAMENTO INF>>
		prcts_tratar_inf(p_cli1            => p_cli,
						 p_f1              => p_f,
						 p_inf1            => v_inf_insert,
						 p_nf1             => p_nf,
						 p_sanea1          => p_sanea,
						 p_cp1             => p_cp,   
						 p_st_t1           => p_st_t,	
						 p_nr_qtde_inf1    => p_nr_qtde_inf);
		-- <<FIM TRATAMENTO INF>>
		-- Altera o registro original
		p_nr_qtde_inf                         := p_nr_qtde_inf + 1;	
        p_inf.update_reg         := 1;
        p_inf.var05              := SUBSTR('prcts_regra_51u:' || p_inf.infst_val_desc || '>>' || p_inf.var05,1,150);
        p_inf.infst_val_desc     := 0;
		v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
														 p_nm_var01      =>  p_inf.emps_cod,
														 p_nm_var02      =>  p_inf.fili_cod,
														 p_nm_var03      =>  p_inf.infst_serie,
														 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
														 p_nm_var05      =>  '|R2015_51|',
														 p_nr_var02      =>  1);  		
	END IF;
  
  END IF;
END;
--/