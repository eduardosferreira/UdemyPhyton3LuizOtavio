-- ;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_corrigi_fili_cod(p_inf          IN OUT NOCOPY c_inf%rowtype, p_nf  IN OUT NOCOPY   c_nf%rowtype)
AS
BEGIN
  
  IF (p_inf.serie = 'IN1')
	AND ( p_inf.fili_cod = '9144')
    AND ( 
		    ( p_inf.infst_dtemiss >= to_date('01/04/2017','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2017','dd/mm/yyyy') ) 
		OR  ( p_inf.infst_dtemiss >= to_date('01/01/2018','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2019','dd/mm/yyyy') ) 
		)
  THEN    

	p_inf.update_reg        := 1;
	p_inf.var05             := SUBSTR('corrigi_fili_cod:' || p_inf.fili_cod || '>>' || p_inf.var05,1,150);
	p_inf.fili_cod          := '0001';

	IF p_nf.fili_cod        != '0001' THEN
		p_nf.update_reg        := 1;
		p_nf.var05             := SUBSTR('corrigi_fili_cod:' || p_nf.fili_cod || '>>' || p_nf.var05,1,150);
		p_nf.fili_cod          := '0001';	
		v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R_CORRIGI_FILI_COD|',
												  p_nr_var01  =>  1); 									
	END IF;
	
  END IF;

END;
--/	