-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_tsh_gf_002(p_nf          IN OUT NOCOPY c_nf%rowtype)
AS

BEGIN
  


	IF TRUNC(p_nf.mnfst_dtemiss, 'MONTH') != TRUNC(p_nf.mnfst_per_ref, 'MONTH')  THEN 
	
	  p_nf.update_reg        := 1;
	  p_nf.var05             := SUBSTR('erro_tsh_gf_002:' || p_nf.mnfst_per_ref || '>>' ||p_nf.var05,1,150);
	  p_nf.mnfst_per_ref     := TRUNC(p_nf.mnfst_dtemiss, 'MONTH');	 	
	  v_nf_rules             := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
									  p_nm_var01  =>  p_nf.emps_cod,
									  p_nm_var02  =>  p_nf.fili_cod,
									  p_nm_var03  =>  p_nf.mnfst_serie,
									  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
									  p_nm_var05  =>  '|ERRO_TSH_GF_002|',
									  p_nr_var01  =>  1); 		  

	END IF;
	

  
END;
--/	