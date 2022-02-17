-- CREATE OR REPLACE 
PROCEDURE prcts_regra_2(p_inf         IN OUT NOCOPY c_inf%rowtype)
AS
  -- MAP_2_REGRA_2_AJUSTE_DESCONTO_COM_BASE
BEGIN
  IF ( p_inf.infst_dtemiss >= to_date('01/01/2015','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') ) 
	AND UPPER(TRANSLATE(p_inf.infst_serie,'x ','x')) IN ('1','UT')
  THEN
	IF NVL(p_inf.infst_isenta_icms,0)    = 0 
		AND NVL(p_inf.infst_base_icms,0) <> 0 
		AND NVL(p_inf.infst_val_desc,0) <> 0 
		AND NVL(p_inf.infst_val_desc,0) = NVL(p_inf.infst_outras_icms,0) 
	THEN
      p_inf.update_reg         := 1;
	  p_inf.var05                                                                := SUBSTR('prcts_regra_2:' || p_inf.infst_outras_icms || '|' || p_inf.infst_val_desc || '|' || p_inf.infst_val_serv || '|' || p_inf.infst_val_cont || '>>' ||p_inf.var05,1,150);
      -- (*) Atribuir ao valor de outras, o valor do desconto, porém negativo (infst_outras_icms := NFST_VAL_DESC * (-1))
      p_inf.infst_outras_icms := NVL(p_inf.infst_val_desc,0) * -1;
      -- (*) Zerar o valor de desconto
      p_inf.infst_val_desc := 0;
      -- (*) Resumarizar os valores do val_serv e val_cont
      p_inf.infst_val_serv := NVL(p_inf.infst_base_icms,0) + NVL(p_inf.infst_outras_icms,0) + NVL(p_inf.infst_isenta_icms,0);
      p_inf.infst_val_cont := NVL(p_inf.infst_val_serv,0);
	  v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
	  									p_nm_var01      =>  p_inf.emps_cod,
	  									p_nm_var02      =>  p_inf.fili_cod,
	  									p_nm_var03      =>  p_inf.infst_serie,
	  									p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
	  									p_nm_var05      =>  '|R2015_2|',
	  									p_nr_var02      =>  1);		  
	  
	  
    END IF;
  END IF;
END;
--/