-- CREATE OR REPLACE 
PROCEDURE prcts_regra_9(p_inf         IN OUT NOCOPY c_inf%rowtype)
AS
  -- MAP_2_REGRA_9_AJUSTE_INF_0_DES_PRE
BEGIN
  IF ( p_inf.infst_dtemiss >= to_date('01/01/2015','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') ) 
	AND UPPER(TRANSLATE(p_inf.infst_serie,'x ','x')) IN ('1','UT')
  THEN
    IF (     nvl(p_inf.INFST_ISENTA_ICMS,0)   = 0
			    AND nvl(p_inf.infst_outras_icms,0)   = 0
			    AND nvl(p_inf.INFST_VAL_DESC,0)     <> 0
			  )
	THEN
      p_inf.update_reg        := 1;
      p_inf.var05                                                                := SUBSTR('prcts_regra_9:' || p_inf.infst_outras_icms || '|' || p_inf.infst_val_desc || '|' || p_inf.infst_val_serv || '|' || p_inf.infst_val_cont || '>>' ||p_inf.var05,1,150);
      -- Nro da Regra 9) Ajuste de Itens Zerados com Desconto Preenchido
 	  -- (*) Atribuir ao valor de outras, o valor do desconto, porém negativo (infst_outras_icms := NFST_VAL_DESC * (-1))
	  p_inf.infst_outras_icms := nvl(p_inf.infst_val_desc,0) * -1;
	  -- (*) Zerar o valor de desconto
	  p_inf.infst_val_desc := 0;		
	  -- (*) Resumarizar os valores do val_serv e val_cont
	  p_inf.infst_val_serv  := nvl(p_inf.infst_outras_icms,0);
	  p_inf.infst_val_cont  := nvl(p_inf.infst_outras_icms,0);	 
	  v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
	  									p_nm_var01      =>  p_inf.emps_cod,
	  									p_nm_var02      =>  p_inf.fili_cod,
	  									p_nm_var03      =>  p_inf.infst_serie,
	  									p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
	  									p_nm_var05      =>  '|R2015_9|',
	  									p_nr_var02      =>  1);			  
    END IF;
  END IF;
END;
--/