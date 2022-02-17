-- CREATE OR REPLACE 
PROCEDURE prcts_regra_6(
    p_f              IN c_f%ROWTYPE,
    p_inf            IN c_inf%rowtype,
    p_nf             IN OUT NOCOPY c_nf%rowtype,
    p_sanea          IN c_sanea%ROWTYPE,
    p_nr_qtde_inf    IN PLS_INTEGER)
AS
  -- 6) - Totalizacao do Mestre
BEGIN

	-- (*) Resumarizar os totais do mestre
	IF p_inf.rowid_inf IS NOT NULL THEN
	
		IF p_nr_qtde_inf <= 1 THEN
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
		
		IF UPPER(TRIM(p_inf.last_reg_nf)) = 'S' THEN
		
			IF ( p_inf.infst_dtemiss >= to_date('01/01/2015','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') ) THEN
				p_nf.mnfst_codh_nf      := rawtohex(dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw(lpad(p_nf.cnpj_cpf, 14, 0) || lpad(p_nf.mnfst_num, 9, 0) || lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_tot, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_basicms, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_icms, '099999999.99'), '.', ''), ',', '')), 12, 0))));
			ELSE
				p_nf.mnfst_codh_nf      := rawtohex(dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw(lpad(p_nf.cnpj_cpf, 14, 0) || lpad(p_nf.mnfst_num, 9, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_tot, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_basicms, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_icms, '099999999.99'), '.', ''), ',', '')), 12, 0)||to_char(p_nf.mnfst_dtemiss, 'yyyymmdd')|| lpad(p_f.fili_cod_cgc, 14, 0))));
			END IF;
			p_nf.mnfst_ind_cont         := p_nf.mnfst_ind_cont_aux;
			
			IF 		nvl(p_nf.mnfst_ind_cont,'')         != nvl(p_sanea.mnfst_ind_cont,'')
				OR  nvl(p_nf.mnfst_codh_nf,0)          != nvl(p_sanea.mnfst_codh_nf,0)
				OR  nvl(p_nf.mnfst_val_tot,0)          != nvl(p_sanea.mnfst_val_tot,0)
				OR  nvl(p_nf.mnfst_val_isentas,0)      != nvl(p_sanea.mnfst_val_isentas,0)
				OR  nvl(p_nf.mnfst_val_outras,0)       != nvl(p_sanea.mnfst_val_outras,0)
				OR  nvl(p_nf.mnfst_val_ser,0)          != nvl(p_sanea.mnfst_val_ser,0)
				OR  nvl(p_nf.mnfst_val_icms,0)         != nvl(p_sanea.mnfst_val_icms,0)
				OR  nvl(p_nf.mnfst_val_basicms,0)      != nvl(p_sanea.mnfst_val_basicms,0)
				OR  nvl(p_nf.mnfst_val_desc,0)         != nvl(p_sanea.mnfst_val_desc,0)
			THEN

				p_nf.var05               := SUBSTR('prcts_regra_6:' || p_sanea.mnfst_val_tot || '|' || p_sanea.mnfst_val_isentas || '|' || p_sanea.mnfst_val_outras || '|' || p_sanea.mnfst_val_ser || '|' || p_sanea.mnfst_val_icms || '|' || p_sanea.mnfst_val_basicms || '|' || p_sanea.mnfst_val_desc || '|' || p_sanea.mnfst_codh_nf || '|' || p_sanea.mnfst_ind_cont || '>>' ||p_nf.var05,1,150);
				p_nf.update_reg          := 1;
				v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R2015_6|',
												  p_nr_var01  =>  1); 				
			END IF;	
			
		END IF;
	
	END IF;

END;
-- /
