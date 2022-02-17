-- CREATE OR REPLACE    
PROCEDURE prcts_regra_47(
    p_inf            IN OUT NOCOPY c_inf%ROWTYPE,
	p_st_t           IN OUT NOCOPY c_st%ROWTYPE,
	p_cli            IN c_cli%ROWTYPE)
AS

 v_pref_servfZT  VARCHAR2(15);

BEGIN

  IF ( p_inf.infst_dtemiss >= to_date('01/01/2017','dd/mm/yyyy') AND p_inf.infst_dtemiss <= to_date('31/12/2019','dd/mm/yyyy') ) 
	 AND ( TRIM(p_cli.cadg_num_conta||p_cli.cadg_tel_contato) IS NULL )
  THEN

	IF 	NVL(p_st_t.sit_reg,0) = 0 THEN
		OPEN c_st (p_emps_cod => p_inf.emps_cod, p_fili_cod => p_inf.fili_cod, p_servtl_cod => p_inf.serv_cod, p_servtl_dat_atua => p_inf.infst_dtemiss);
		FETCH c_st INTO p_st_t;
		IF c_st%NOTFOUND THEN
		  CLOSE c_st;
		  p_st_t.sit_reg := 3;
		  raise_application_error (-20343, 'Servico nao encontrado! ' || ' >> emps_cod: ' || p_inf.emps_cod || ' >> fili_cod: ' || p_inf.fili_cod || ' >> serv_cod: ' || p_inf.serv_cod || ' >> infst_dtemiss: ' || p_inf.infst_dtemiss || ' >> infst_num: ' || p_inf.infst_num || ' >> infst_serie: ' || p_inf.infst_serie);
		ELSE
		  p_st_t.sit_reg := 1;
		  p_st_t.SERVTL_DAT_ATUA := p_st_t.servtl_dat_atual;
		END IF;
		CLOSE c_st;
	ELSIF p_st_t.sit_reg = 3 THEN
		RETURN;
	END IF;
	
	v_pref_servfZT :=  p_inf.serie || 'ZT';
	
	IF SUBSTR(p_st_t.clasfi_cod,3) != '99' 
	  AND  p_st_t.servtl_cod is not null 
	  AND (p_st_t.servtl_cod NOT LIKE  v_pref_servfZT || '%'
			OR  TRIM(p_st_t.servtl_tip_utiliz) != '6'
			OR  TRIM(p_st_t.clasfi_cod)        != '0599'
		   ) 
	THEN 
      p_st_t.update_reg := 1;
	  p_st_t.var05             := SUBSTR('prcts_regra_47i:'|| v_pref_servfZT ||':' || p_st_t.servtl_cod || '|' || p_st_t.clasfi_cod || '|' || p_st_t.servtl_tip_utiliz || '>>'|| p_st_t.VAR05,1,150);
	  p_st_t.servtl_cod        := TRIM(SUBSTR(fccts_retira_caracter(p_ds_ddo => p_st_t.servtl_cod, p_ds_serie => p_inf.serie),1,60));
	  p_st_t.servtl_cod        := trim(SUBSTR(v_pref_servfZT || p_st_t.servtl_cod,1,60));
	  p_st_t.servtl_tip_utiliz := '6';
	  p_st_t.clasfi_cod        := '0599';	  
	  IF p_inf.serv_cod  != p_st_t.servtl_cod THEN
		  p_inf.update_reg        := 1;
		  p_inf.var05      := substr('prcts_regra_47u<1>:' || p_inf.serv_cod  || '|' || p_st_t.servtl_cod  || '>>' ||p_inf.VAR05,1,150) ;	 		
		  p_inf.serv_cod   := p_st_t.servtl_cod;
		  v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
													 p_nm_var01      =>  p_inf.emps_cod,
													 p_nm_var02      =>  p_inf.fili_cod,
													 p_nm_var03      =>  p_inf.infst_serie,
													 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
													 p_nm_var05      =>  '|R2017_47|',
													 p_nr_var02      =>  1);
	  END IF;
	  v_st_t_rules      := fncts_add_var(p_ds_rules     =>  v_st_t_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|R2017_47|',
										p_nr_var05      =>  1);	 	  
	END IF;

  END IF;

END;
-- /