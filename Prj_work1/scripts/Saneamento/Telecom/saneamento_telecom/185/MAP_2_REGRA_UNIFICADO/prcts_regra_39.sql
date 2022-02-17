-- CREATE OR REPLACE    
PROCEDURE prcts_regra_39(
    p_inf            IN OUT NOCOPY c_inf%rowtype,
	p_st_t           IN OUT NOCOPY c_st%ROWTYPE)
AS

 v_pref_servfA  VARCHAR2(15);
 v_pref_servfB  VARCHAR2(15);
 v_pref_servfC  VARCHAR2(15);
 v_pref_servfD  VARCHAR2(15);
 v_pref_servfE  VARCHAR2(15);
 v_pref_servfF  VARCHAR2(15);
 v_pref_servfZ  VARCHAR2(15);
 v_pref_servfZP VARCHAR2(15);
 v_pref_servfZN VARCHAR2(15);
 v_pref_servf1  VARCHAR2(15);
 v_pref_servf2  VARCHAR2(15);	 
 v_serv_cod openrisow.item_nftl_serv.serv_cod%type; 

BEGIN
  IF ( NVL(p_inf.cfop,'_0_') != '0000')
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
	v_pref_servfA  :=  p_inf.serie || 'A';
	v_pref_servfB  :=  p_inf.serie || 'B';
	v_pref_servfC  :=  p_inf.serie || 'C';
	v_pref_servfD  :=  p_inf.serie || 'D';
	v_pref_servfE  :=  p_inf.serie || 'E';
	v_pref_servfF  :=  p_inf.serie || 'F';
	v_pref_servfZ  :=  p_inf.serie || 'Z';
	v_pref_servfZP :=  p_inf.serie || 'ZP';
	v_pref_servfZN :=  p_inf.serie || 'ZN';
	v_pref_servf1  :=  p_inf.serie || 'C08';
	v_pref_servf2  :=  p_inf.serie || 'C09';
	v_serv_cod     := UPPER(TRIM(TRANSLATE(TRIM(p_inf.serv_cod), '" ', '"')));		
	v_serv_cod     := trim(SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p_st_t.servtl_cod,v_pref_servfZP,''),v_pref_servfZN,''),v_pref_servfZ,''),v_pref_servfA,''),v_pref_servfB,''),v_pref_servfC,''),v_pref_servfD,''),v_pref_servfE,''),v_pref_servfF,''),1,60));
	IF     p_st_t.clasfi_cod LIKE '08%' AND p_st_t.servtl_cod IS NOT NULL AND v_serv_cod NOT LIKE v_pref_servf1 || '%' THEN
	  p_st_t.update_reg := 1;
	  p_st_t.var05                                                                                                         := SUBSTR('prcts_regra_39i:'|| v_pref_servf1 ||':' || p_st_t.servtl_cod || '|' || p_st_t.clasfi_cod || '|' || p_st_t.servtl_tip_utiliz || '>>'|| p_st_t.VAR05,1,150);
	  p_st_t.servtl_cod                                                                                                    := trim(SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p_st_t.servtl_cod,v_pref_servf1,''),v_pref_servf2,''),v_pref_servfZP,''),v_pref_servfZN,''),v_pref_servfZ,''),v_pref_servfA,''),v_pref_servfB,''),v_pref_servfC,''),v_pref_servfD,''),v_pref_servfE,''),v_pref_servfF,''),1,60));
	  p_st_t.servtl_cod                                                                                                    := trim(SUBSTR(v_pref_servf1 || p_st_t.servtl_cod,1,60));
	  p_st_t.clasfi_cod                                                                                                    := '0199';
	  p_st_t.servtl_tip_utiliz                                                                                             := '6';
	  v_st_t_rules      := fncts_add_var(p_ds_rules     =>  v_st_t_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|R2016_39|',
										p_nr_var05      =>  1);	 	  
	ELSIF p_st_t.clasfi_cod LIKE '09%' AND p_st_t.servtl_cod IS NOT NULL AND v_serv_cod NOT LIKE v_pref_servf2 || '%' THEN
	  p_st_t.update_reg := 1;
	  p_st_t.var05                                                                                                         := SUBSTR('prcts_regra_39i:'|| v_pref_servf2 ||':' || p_st_t.servtl_cod || '|' || p_st_t.clasfi_cod || '|' || p_st_t.servtl_tip_utiliz || '>>'|| p_st_t.VAR05,1,150);
	  p_st_t.servtl_cod                                                                                                    := trim(SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p_st_t.servtl_cod,v_pref_servf1,''),v_pref_servf2,''),v_pref_servfZP,''),v_pref_servfZN,''),v_pref_servfZ,''),v_pref_servfA,''),v_pref_servfB,''),v_pref_servfC,''),v_pref_servfD,''),v_pref_servfE,''),v_pref_servfF,''),1,60));
	  p_st_t.servtl_cod                                                                                                    := trim(SUBSTR(v_pref_servf2 || p_st_t.servtl_cod,1,60));
	  p_st_t.clasfi_cod                                                                                                    := '0199';
	  p_st_t.servtl_tip_utiliz                                                                                             := '6';
	  v_st_t_rules      := fncts_add_var(p_ds_rules     =>  v_st_t_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|R2016_39|',
										p_nr_var05      =>  1);	 	  
	END IF;
	IF p_inf.serv_cod  != p_st_t.servtl_cod THEN
	  p_inf.update_reg        := 1;
	  p_inf.var05      := substr('prcts_regra_39u<1>:' || p_inf.serv_cod  || '|' || p_st_t.servtl_cod  || '>>' ||p_inf.VAR05,1,150) ;	 		
	  p_inf.serv_cod   := p_st_t.servtl_cod;
	  v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|R2016_39|',
										p_nr_var02      =>  1);	  	  
	END IF;

  END IF;

END;
-- /