-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_6_37(p_cli            IN OUT NOCOPY c_cli%ROWTYPE) AS
	
BEGIN

	IF NVL(trim(p_cli.cadg_grp_tensao), '00') <> '00' THEN
		p_cli.update_reg_comp := 1;						
		p_cli.cadg_grp_tensao := '00';	
		v_cli_rules_comp         := fncts_add_var(p_ds_rules  =>  v_cli_rules_comp, 
												  p_nm_var01  =>  v_nf.emps_cod,
						                          p_nm_var02  =>  v_nf.fili_cod,
						                          p_nm_var03  =>  v_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(v_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|ERRO_6_37|',
												  p_nr_var04  =>  1); 							  
	END IF;

END;
--/	