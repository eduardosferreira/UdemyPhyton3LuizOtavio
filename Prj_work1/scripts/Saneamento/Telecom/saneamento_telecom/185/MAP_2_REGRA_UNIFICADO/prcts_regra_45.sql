-- CREATE OR REPLACE 
PROCEDURE prcts_regra_45(p_inf         IN OUT NOCOPY c_inf%rowtype)
AS
  -- MAP_2_REGRA_45
  v_exists PLS_INTEGER := 0;
BEGIN
  IF (p_inf.infst_dtemiss >= to_date('01/01/2017','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2020','dd/mm/yyyy') ) 
  THEN
	
	IF NVL(p_inf.mdoc_cod,0) in (21,22)
     AND UPPER(TRANSLATE(p_inf.serie,'x ','x')) IN  ('UK','TV2','BK')
     AND p_inf.uf <>'EX'
     AND NVL(p_inf.infst_isenta_icms,0) <> 0                   
	   AND NVL(p_inf.infst_outras_icms,0) = 0
     AND p_inf.infst_base_icms = 0
     AND p_inf.infst_val_icms = 0
     AND p_inf.infst_aliq_icms = 0
	THEN
    
      BEGIN
          SELECT NVL(count(1) ,0) INTO v_exists
          FROM gfcadastro.tmp_st_05 tmp
          WHERE UPPER(TRIM(tmp.serv_cod))  = TRIM((UPPER(p_inf.serv_cod))) ;                          
      EXCEPTION
         WHEN OTHERS THEN
             v_exists := 0;
      END;
      
      IF v_exists = 0 THEN
        
        BEGIN
            SELECT NVL(count(1) ,0) INTO v_exists
            FROM gfcadastro.tmp_cgc_cpf_01 tmp
            WHERE UPPER(TRIM(tmp.cgc_cpf))  = TRIM((UPPER(p_inf.cgc_cpf)))  ;                          
        EXCEPTION
           WHEN OTHERS THEN
               v_exists := 0;
        END;
         
        IF v_exists = 0 THEN
          
          p_inf.update_reg        := 1;
          p_inf.var05             := substr('prcts_regra_45u:' || p_inf.estb_cod || '|' || p_inf.infst_tribicms || '|' || p_inf.infst_outras_icms || '|' || p_inf.infst_isenta_icms || '>>' ||p_inf.var05,1,150);
          p_inf.infst_outras_icms := p_inf.infst_isenta_icms;
          p_inf.infst_isenta_icms := 0;  
          p_inf.estb_cod := 90;  
          p_inf.infst_tribicms:= 'P';
	      v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|R2015_45|',
										p_nr_var02      =>  1);	 		  
     
        END IF;
      
      END IF;   
      
   END IF;
	
  END IF;
END;
--/
