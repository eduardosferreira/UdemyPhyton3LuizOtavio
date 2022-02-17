-- CREATE OR REPLACE 
PROCEDURE prcts_regra_34(p_inf         IN OUT NOCOPY c_inf%rowtype)
AS
  -- MAP_2_REGRA_34_TRATAR_VAL_TOT_ZERADO
BEGIN
  IF ( p_inf.infst_dtemiss >= to_date('01/01/2015','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') ) 
  THEN
	
	IF  NVL(p_inf.infst_val_cont,0) = 0
		  AND NVL(p_inf.infst_val_serv,0) = 0 
		  AND NVL(p_inf.infst_val_desc,0) = 0
		  AND NVL(p_inf.infst_base_icms,0) = 0
		  AND NVL(p_inf.infst_val_icms,0) = 0
		  AND NVL(p_inf.infst_isenta_icms,0) = 0
		  AND NVL(p_inf.infst_outras_icms,0) = 0
		  AND (p_inf.infst_tribicms <> 'P' OR p_inf.estb_cod <> '90'or p_inf.cfop <> '0000')
	THEN
      p_inf.update_reg        := 1;
      p_inf.var05             := substr('prcts_regra_34u:' || p_inf.infst_tribicms || '|' || p_inf.estb_cod || '|' || p_inf.infst_aliq_icms || '|' || p_inf.cfop  || '>>' ||p_inf.var05,1,150);
	  p_inf.infst_tribicms    := 'P';
	  p_inf.estb_cod          := '90';
	  p_inf.cfop              := '0000';
	  p_inf.infst_aliq_icms   := '0';
	  v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
	  							p_nm_var01      =>  p_inf.emps_cod,
	  							p_nm_var02      =>  p_inf.fili_cod,
	  							p_nm_var03      =>  p_inf.infst_serie,
	  							p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
	  							p_nm_var05      =>  '|R2015_34|',
	  							p_nr_var02      =>  1);			
	  
    END IF;
	
  END IF;
END;
--/