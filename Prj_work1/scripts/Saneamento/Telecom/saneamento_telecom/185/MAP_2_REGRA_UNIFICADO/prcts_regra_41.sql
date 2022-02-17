-- CREATE OR REPLACE 
PROCEDURE prcts_regra_41(p_inf            IN OUT NOCOPY c_inf%rowtype)
AS
BEGIN

    IF    NVL(p_inf.infst_isenta_icms,0) <> 0 
	  AND (NVL(p_inf.infst_base_icms,0) <> 0 or NVL(p_inf.infst_val_icms,0) <> 0)
	  AND NVL(p_inf.infst_val_red,0) <> NVL(p_inf.infst_isenta_icms,0)
	  AND NVL(p_inf.infst_val_cont,0) = NVL(p_inf.infst_base_icms,0) + NVL(p_inf.infst_isenta_icms,0) 
	  AND NVL(p_inf.infst_outras_icms,0) = 0 
	THEN
        p_inf.update_reg         := 1;
		p_inf.var05              := substr('prcts_regra_41u:' || p_inf.infst_val_red       || '|' || p_inf.infst_isenta_icms   || '>>' ||p_inf.VAR05,1,150) ;	
        p_inf.infst_val_red      := p_inf.infst_isenta_icms;		
	    v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
													 p_nm_var01      =>  p_inf.emps_cod,
													 p_nm_var02      =>  p_inf.fili_cod,
													 p_nm_var03      =>  p_inf.infst_serie,
													 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
													 p_nm_var05      =>  '|R2016_41|',
													 p_nr_var02      =>  1);			
	END IF;

END;
--/