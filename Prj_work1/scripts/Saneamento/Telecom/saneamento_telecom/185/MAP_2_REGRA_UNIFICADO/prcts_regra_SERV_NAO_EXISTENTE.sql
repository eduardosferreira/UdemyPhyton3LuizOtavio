PROCEDURE prcts_regra_SERV_NAO_EXISTENTE(
    p_inf            IN c_inf%rowtype,
	p_st_t           IN OUT NOCOPY c_st%ROWTYPE)
AS
	v_idx_regra_aux c_st_i%TYPE;
    v_st             openrisow.servico_telcom%ROWTYPE; 
	v_st_t_regra_aux c_st%ROWTYPE;


BEGIN
  v_st_t_regra_aux := p_st_t;
  IF ( v_st_t_regra_aux.servtl_cod  IS NULL AND (p_inf.serv_cod  IS NOT NULL  AND p_inf.rowid_inf IS NOT NULL) )
  THEN


	  v_idx_regra_aux := v_st_t_regra_aux.EMPS_COD || '|'|| v_st_t_regra_aux.FILI_COD || '|' || v_st_t_regra_aux.SERVTL_COD||'|'||v_st_t_regra_aux.servtl_desc;	
	  IF 	NVL(v_bk_st_t3.COUNT,0) > 0 THEN
		IF  v_bk_st_t3.exists(v_idx_regra_aux) THEN
			IF v_bk_st_t3(v_idx_regra_aux).servtl_dat_atua <= p_inf.infst_dtemiss  THEN
				v_st_t_regra_aux := v_bk_st_t3(v_idx_regra_aux); 
				p_st_t := v_st_t_regra_aux;
				RETURN;
			END IF;	
		END IF;		  
      END IF;	

 
	  OPEN c_st (p_emps_cod => p_inf.emps_cod, p_fili_cod => p_inf.fili_cod, p_servtl_cod => p_inf.serv_cod, p_servtl_dat_atua => p_inf.infst_dtemiss);
	  FETCH c_st INTO v_st_t_regra_aux;
	  IF c_st%NOTFOUND THEN	    
	    CLOSE c_st;	

        RETURN;
				
	  ELSE
		CLOSE c_st;
		
        v_st_t_regra_aux.update_reg      := 1;
	    v_st_t_regra_aux.sit_reg         := 1;
        v_st_t_regra_aux.emps_cod        := p_inf.emps_cod;
		v_st_t_regra_aux.fili_cod        := p_inf.fili_cod;
		v_st_t_regra_aux.SERVTL_DAT_ATUA := TRUNC(v_st_t_regra_aux.SERVTL_DAT_ATUAL,'MONTH'); --- primeiro dia do mes.
		v_st_t_regra_aux.var05           := SUBSTR('SERVICO_INEXISTENTE[1]:' || v_st_t_regra_aux.SERVTL_DAT_ATUA || '>>'|| v_st_t_regra_aux.VAR05,1,150);
		
		--- validar v_st_t_regra_aux.clasfi_cod
		---
		IF v_st_t_regra_aux.clasfi_cod IS NULL THEN
		RETURN;
		END IF;
		
		v_st.clasfi_cod        := v_st_t_regra_aux.clasfi_cod;
		v_st.servtl_tip_utiliz := NULL;
		BEGIN
			SELECT TMP.servtl_tip_utiliz
			INTO v_st.servtl_tip_utiliz
			FROM
			(SELECT
				/*+ parallel(8) */
				A.CODIGO                 AS clasfi_cod,
				A.CODIGO_TIPO_UTILIZACAO AS servtl_tip_utiliz
			FROM gfcadastro.tmp_tb_de_para_conv_115_rev a
			) TMP
			WHERE TMP.clasfi_cod = v_st_t_regra_aux.clasfi_cod AND ROWNUM < 2;		
		EXCEPTION
		WHEN OTHERS THEN 
			v_st.servtl_tip_utiliz := NULL;
		END;
		IF TRIM(v_st.servtl_tip_utiliz) IS NOT NULL THEN
			v_st_t_regra_aux.servtl_tip_utiliz := v_st.servtl_tip_utiliz;
		END IF;
		
	    v_st_t_rules      := fncts_add_var(p_ds_rules     =>  v_st_t_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|SERVICO_INEXISTENTE|',
										p_nr_var05      =>  1);	 	

		p_st_t := 	v_st_t_regra_aux;									
		
	  END IF;  		
      
					  
  END IF;

END;
-- /