-- CREATE OR REPLACE 
PROCEDURE prcts_regra_CFOP_5_6(
    p_f              IN c_f%ROWTYPE,
    p_inf            IN OUT NOCOPY c_inf%ROWTYPE)
AS

BEGIN


	IF p_inf.infst_ind_canc = 'N'
	   AND ((p_f.unfe_sig =  p_inf.uf AND SUBSTR(p_inf.cfop,1,1) = '6') 
		 OR (p_f.unfe_sig <> p_inf.uf AND SUBSTR(p_inf.cfop,1,1) = '5'))
	THEN	 
		p_inf.update_reg := 1;
		p_inf.var05              := SUBSTR('prcts_regra_CFOP_5_6'||':'|| p_inf.CFOP ||'>>'||p_inf.var05,1,150);
		p_inf.CFOP               := (CASE WHEN SUBSTR(p_inf.CFOP,1,1) = '6' THEN '5' ELSE '6' END) || SUBSTR(p_inf.CFOP, 2);
	    v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
									p_nm_var01      =>  p_inf.emps_cod,
									p_nm_var02      =>  p_inf.fili_cod,
									p_nm_var03      =>  p_inf.infst_serie,
									p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
									p_nm_var05      =>  '|CFOP_5_6|',
									p_nr_var02      =>  1);	 							  
	END IF;


END;
-- /
