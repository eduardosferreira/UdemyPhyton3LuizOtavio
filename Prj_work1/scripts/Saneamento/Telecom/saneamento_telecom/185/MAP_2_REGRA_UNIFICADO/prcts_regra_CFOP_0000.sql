-- CREATE OR REPLACE    
PROCEDURE prcts_regra_CFOP_0000(
    p_inf            IN OUT NOCOPY c_inf%rowtype,
	p_st_t           IN OUT NOCOPY c_st%ROWTYPE)
AS

 v_pref_servfZP VARCHAR2(15);
 v_pref_servfZN VARCHAR2(15);
 v_serv_cod openrisow.item_nftl_serv.serv_cod%type;  
BEGIN
  IF ( NVL(p_inf.cfop,'_0_') = '0000' AND p_inf.rowid_inf IS NOT NULL )
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
		  p_st_t.SERVTL_DAT_ATUA := p_st_t.SERVTL_DAT_ATUAL;
		END IF;
		CLOSE c_st;
	ELSIF p_st_t.sit_reg = 3 THEN
		RETURN;
	END IF;	
	
	v_pref_servfZP := p_inf.serie || 'ZP';
	v_pref_servfZN := p_inf.serie || 'ZN';
	v_serv_cod       := TRIM(SUBSTR(fccts_retira_caracter(p_ds_ddo => p_st_t.servtl_cod, p_ds_serie => p_inf.serie),1,60)); 
	IF NVL(p_inf.infst_val_cont,0) >= 0 AND p_st_t.clasfi_cod != '0899' and p_st_t.servtl_cod is not null and p_st_t.servtl_cod NOT LIKE  v_pref_servfZP || '%' THEN 
      p_st_t.update_reg := 1;
	  p_st_t.var05                                                                                                         := SUBSTR('prcts_regra_CFOP_0000:'|| v_pref_servfZP ||':' || p_st_t.servtl_cod || '|' || p_st_t.clasfi_cod || '|' || p_st_t.servtl_tip_utiliz || '>>'|| p_st_t.VAR05,1,150);
	  p_st_t.servtl_cod                                                                                                    := trim(SUBSTR(v_pref_servfZP || v_serv_cod,1,60));
	  p_st_t.clasfi_cod                                                                                                    := '0899';
	  p_st_t.servtl_tip_utiliz                                                                                             := '6';
	  v_st_t_rules      := fncts_add_var(p_ds_rules     =>  v_st_t_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|CFOP_0000|',
										p_nr_var05      =>  1);	 
	ELSIF NVL(p_inf.infst_val_cont,0) < 0 and p_st_t.clasfi_cod != '0999' and p_st_t.servtl_cod is not null and p_st_t.servtl_cod NOT LIKE  v_pref_servfZN || '%' THEN
      p_st_t.update_reg := 1;
	  p_st_t.var05                                                                                                         := SUBSTR('prcts_regra_CFOP_0000:'|| v_pref_servfZN ||':' || p_st_t.servtl_cod || '|' || p_st_t.clasfi_cod || '|' || p_st_t.servtl_tip_utiliz || '>>'|| p_st_t.VAR05,1,150);
	  p_st_t.servtl_cod                                                                                                    := trim(SUBSTR(v_pref_servfZN || v_serv_cod,1,60));
	  p_st_t.clasfi_cod                                                                                                    := '0999';
	  p_st_t.servtl_tip_utiliz                                                                                             := '6';
	  v_st_t_rules      := fncts_add_var(p_ds_rules     =>  v_st_t_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|CFOP_0000|',
										p_nr_var05      =>  1);	 
	END IF;
	IF p_inf.serv_cod  != p_st_t.servtl_cod THEN
	  p_inf.update_reg := 1;
	  p_inf.var05      := substr('prcts_regra_CFOP_0000u:' || p_inf.serv_cod  || '|' || p_st_t.servtl_cod  || '>>' ||p_inf.VAR05,1,150) ;	 		
	  p_inf.serv_cod   := p_st_t.servtl_cod;
	  v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|CFOP_0000|',
										p_nr_var02      =>  1);	  
	END IF;
	
  END IF;

END;
-- /