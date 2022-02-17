-- CREATE OR REPLACE 
PROCEDURE prcts_regra_59(
    p_inf            IN OUT NOCOPY c_inf%rowtype,
    p_nf             IN c_nf%rowtype)
AS

BEGIN

	IF 	p_nf.rowid_nf  IS NOT NULL
		AND p_inf.rowid_inf IS NOT NULL 
		AND NVL(p_nf.MNFST_VAL_BASICMS,0) = 0
		AND NVL(p_nf.MNFST_VAL_ICMS,0) = 0
		AND NVL(p_inf.INFST_VAL_CONT,0) = NVL(p_inf.INFST_VAL_SERV,0)
		AND NVL(p_inf.INFST_OUTRAS_ICMS,0) = NVL(p_inf.INFST_VAL_CONT,0)
		AND NVL(p_inf.INFST_BASE_ICMS,0) = 0
		AND NVL(p_inf.INFST_VAL_ICMS,0) = 0
		AND NVL(p_inf.INFST_ALIQ_COFINS,0) = 3
		AND NVL(p_inf.INFST_ALIQ_PIS,0) = 0.65
		AND NVL(p_inf.CFOP,' ') != '0000'
	THEN
		p_inf.update_reg          := 1;
		p_inf.var05               := SUBSTR('prcts_regra_59:' || p_inf.INFST_TIP_ISENCAO || '|' || p_inf.INFST_ISENTA_ICMS || '|' || p_inf.INFST_OUTRAS_ICMS || '|' || p_inf.ESTB_COD || '|' || p_inf.INFST_TRIBICMS || '>>' ||p_nf.var05,1,150);
				

		p_inf.INFST_TIP_ISENCAO := '99';
		p_inf.INFST_ISENTA_ICMS := p_inf.INFST_OUTRAS_ICMS; 
		p_inf.INFST_OUTRAS_ICMS := 0; 
		p_inf.ESTB_COD 			:= '40';
		p_inf.INFST_TRIBICMS 	:= 'N'; 

	    v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|R2015_59|',
										p_nr_var02      =>  1);	 			
			
	
	END IF;

END;
-- /
