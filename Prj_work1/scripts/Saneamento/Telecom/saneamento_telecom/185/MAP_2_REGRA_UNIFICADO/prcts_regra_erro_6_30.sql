-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_6_30(p_inf          IN OUT NOCOPY c_inf%rowtype)
AS

BEGIN
  

	IF p_inf.infst_ind_canc = 'N' 
		AND p_inf.infst_serie IN ('U', '05', '7') 
		AND (LTRIM(TRIM(p_inf.infst_num_contr), '0') IS NULL OR LENGTH(TRIM(p_inf.INFST_NUM_CONTR)) <> 15) 
	THEN
		p_inf.update_reg        := 1;
		p_inf.var05             := SUBSTR('erro_6_30:' || p_inf.infst_num_contr || '>>' || p_inf.var05,1,150);
		p_inf.infst_num_contr   := LPAD(NVL(TRIM(p_inf.infst_num_contr), '0'), 15, '0');
		v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
						        			p_nm_var01      =>  p_inf.emps_cod,
						        			p_nm_var02      =>  p_inf.fili_cod,
						        			p_nm_var03      =>  p_inf.infst_serie,
						        			p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
						        			p_nm_var05      =>  '|ERRO_6_30|',
						        			p_nr_var02      =>  1);								
	END IF;
	


END;
--/	