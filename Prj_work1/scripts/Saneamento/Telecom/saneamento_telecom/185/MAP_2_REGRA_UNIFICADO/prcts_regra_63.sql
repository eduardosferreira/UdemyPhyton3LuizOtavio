-- CREATE OR REPLACE 
PROCEDURE prcts_regra_63(p_inf         IN OUT NOCOPY c_inf%rowtype)
AS
	v_SERVTL_COD GFCADASTRO.tb_tmp_serv_jur_mul.SERVTL_COD%TYPE;
BEGIN

	IF NVL(p_inf.ESTB_COD,' ') = '40'
	   AND NVL(p_inf.INFST_OUTRAS_ICMS,0) 	=  0
	   AND NVL(p_inf.INFST_ISENTA_ICMS,0) 	<> 0
	   AND NVL(p_inf.INFST_BASE_ICMS,0) = 0
	   AND NVL(p_inf.INFST_VAL_ICMS,0) = 0
	   AND NVL(p_inf.INFST_TRIBICMS,' ') <> 'P'
    THEN
	BEGIN
	
		BEGIN
			SELECT SERVTL_COD INTO v_SERVTL_COD FROM GFCADASTRO.tb_tmp_serv_jur_mul WHERE SERVTL_COD = p_inf.SERV_COD AND ROWNUM < 2;
		EXCEPTION
		WHEN OTHERS THEN 
			v_SERVTL_COD := NULL;
		END;	

		IF v_SERVTL_COD IS NOT NULL 
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'JURO%' 
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'MULT.%' 
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'MULTA%'
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'AUTU%'
		THEN
		
			p_inf.update_reg         := 1;
			p_inf.var05              := SUBSTR('prcts_regra_63(1):' || p_inf.CFOP || p_inf.ESTB_COD || '|' || p_inf.INFST_TRIBICMS || '|' || p_inf.INFST_OUTRAS_ICMS || '|' || p_inf.INFST_ISENTA_ICMS || '>>' || p_inf.var05,1,150);
			
			p_inf.ESTB_COD 			 := '90';
			p_inf.INFST_TRIBICMS	 := 'P';
			p_inf.INFST_OUTRAS_ICMS  := p_inf.INFST_ISENTA_ICMS;
			p_inf.INFST_ISENTA_ICMS  := 0;
			p_inf.CFOP 				 := '0000';
		
			v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
												 p_nm_var01      =>  p_inf.emps_cod,
												 p_nm_var02      =>  p_inf.fili_cod,
												 p_nm_var03      =>  p_inf.infst_serie,
												 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
												 p_nm_var05      =>  '|R2015_63|',
												 p_nr_var02      =>  1);  	  
		END IF;
	EXCEPTION
	WHEN OTHERS THEN 
		v_SERVTL_COD := NULL;
	END;	
	END IF;
	
	IF  NVL(p_inf.ESTB_COD,' ') = '90'
		AND NVL(p_inf.INFST_OUTRAS_ICMS,0) <>  0
		AND NVL(p_inf.INFST_ISENTA_ICMS,0) =  0
		AND NVL(p_inf.INFST_BASE_ICMS,0) =  0
		AND NVL(p_inf.INFST_VAL_ICMS,0) =  0
		AND NVL(p_inf.INFST_TRIBICMS,' ') = 'P'
		AND NVL(p_inf.CFOP,' ')  <> '0000'
    THEN
	BEGIN
		BEGIN
			SELECT SERVTL_COD INTO v_SERVTL_COD FROM GFCADASTRO.tb_tmp_serv_jur_mul WHERE SERVTL_COD = p_inf.SERV_COD AND ROWNUM < 2;
		EXCEPTION
		WHEN OTHERS THEN 
			v_SERVTL_COD := NULL;
		END;	

		IF v_SERVTL_COD IS NOT NULL 
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'JURO%' 
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'MULT.%' 
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'MULTA%'
		   OR UPPER(p_inf.INFST_DSC_COMPL) LIKE 'AUTU%'
		THEN

			p_inf.update_reg         := 1;
			p_inf.var05              := SUBSTR('prcts_regra_63(2):' || p_inf.CFOP || p_inf.ESTB_COD || '|' || p_inf.INFST_TRIBICMS || '|' || p_inf.INFST_OUTRAS_ICMS || '|' || p_inf.INFST_ISENTA_ICMS || '>>' || p_inf.var05,1,150);
			
			p_inf.CFOP 				 := '0000';
		
			v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
												 p_nm_var01      =>  p_inf.emps_cod,
												 p_nm_var02      =>  p_inf.fili_cod,
												 p_nm_var03      =>  p_inf.infst_serie,
												 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
												 p_nm_var05      =>  '|R2015_63|',
												 p_nr_var02      =>  1);  	  
		END IF;
	EXCEPTION
	WHEN OTHERS THEN 
		v_SERVTL_COD := NULL;
	END;	
	END IF;
	
    
END;
--/