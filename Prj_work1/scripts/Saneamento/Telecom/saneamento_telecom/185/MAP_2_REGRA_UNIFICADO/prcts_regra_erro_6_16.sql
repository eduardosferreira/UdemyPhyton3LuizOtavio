-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_6_16(p_inf          IN OUT NOCOPY c_inf%rowtype, p_nf  IN OUT NOCOPY   c_nf%rowtype, p_sanea  IN  c_sanea%rowtype)
AS
BEGIN
  
	IF UPPER(TRIM(p_sanea.last_reg_nf)) = 'S' THEN
        IF p_nf.mnfst_ind_canc != p_sanea.mnfst_ind_canc_certo AND p_sanea.mnfst_ind_canc_certo IN ('S', 'R', 'C', 'N') THEN
			p_nf.update_reg        := 1;
			p_nf.var05             := SUBSTR('erro_6_16:' || p_nf.mnfst_ind_canc || '>>' || p_nf.var05,1,150);		
			p_nf.mnfst_ind_canc    := p_sanea.mnfst_ind_canc_certo;
			
			v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
													  p_nm_var01  =>  p_nf.emps_cod,
													  p_nm_var02  =>  p_nf.fili_cod,
													  p_nm_var03  =>  p_nf.mnfst_serie,
													  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
													  p_nm_var05  =>  '|ERRO_6_16|',
													  p_nr_var01  =>  1); 										

        ELSIF p_sanea.mnfst_ind_canc_certo NOT IN ('S', 'R', 'C', 'N') THEN
          prcts_stop (' * ROWID_NF: ' || p_nf.rowid_nf || ' >> <<erro_6_16>> nota invalido , não pertence - > s, r, c, n >> mnfst_ind_canc: ' || p_nf.mnfst_ind_canc || ' >> infst_ind_canc: ' || p_inf.infst_ind_canc );
        END IF;	
	END IF;
	
	IF p_inf.infst_ind_canc != p_sanea.mnfst_ind_canc_certo AND p_sanea.mnfst_ind_canc_certo IN ('S', 'R', 'C', 'N') THEN 
		p_inf.update_reg        := 1;
		p_inf.var05             := SUBSTR('erro_6_16:' || p_inf.infst_ind_canc || '>>' || p_inf.var05,1,150);
		p_inf.infst_ind_canc    := p_sanea.mnfst_ind_canc_certo;
		
		v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
												 p_nm_var01      =>  p_inf.emps_cod,
		                                         p_nm_var02      =>  p_inf.fili_cod,
		                                         p_nm_var03      =>  p_inf.infst_serie,
						                         p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
		                                         p_nm_var05      =>  '|ERRO_6_16|',
						                         p_nr_var02      =>  1);
	END IF;
	
END;
--/	