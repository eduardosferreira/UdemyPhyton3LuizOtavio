PROCEDURE prcts_regra_24(p_inf         IN OUT NOCOPY c_inf%rowtype)
AS
	v_serv_cod 				openrisow.item_nftl_serv.serv_cod%type; 
	v_fl_exists 			PLS_INTEGER := 0;
BEGIN
	-- Regra 
	IF  ((NVL(p_inf.INFST_OUTRAS_ICMS,0)      <> 0 
	   or NVL(p_inf.INFST_ISENTA_ICMS,0)      <> 0))
		AND NVL(p_inf.INFST_VAL_CONT,0)        = NVL(p_inf.INFST_VAL_SERV,0)
		AND NVL(p_inf.INFST_BASE_ICMS,0)       = 0
		AND NVL(p_inf.INFST_VAL_ICMS,0)        = 0
		AND NVL(p_inf.INFST_VAL_DESC,0)        = 0
		AND (  NVL(p_inf.CFOP,'_')            <> '0000' 
			OR NVL(p_inf.INFST_TRIBICMS,'_')  <> 'P' 
			OR NVL(p_inf.ESTB_COD,'_')        <> '90' 
			OR NVL(p_inf.INFST_ISENTA_ICMS,0) <> 0 
			OR NVL(p_inf.INFST_ALIQ_ICMS,0)   <> 0)
	THEN

		v_serv_cod       := TRIM(SUBSTR(fccts_retira_caracter(p_ds_ddo => p_inf.serv_cod, p_ds_serie => p_inf.serie),1,60));  
		BEGIN
			SELECT /*+ first_rows(1)*/ NVL(COUNT(1),0) INTO v_fl_exists FROM DUAL
			WHERE EXISTS (
				SELECT 1 FROM GFCADASTRO.TMP_ST_04 WHERE UPPER(TRIM(SERVTL_COD))= TRIM(UPPER(p_inf.serv_cod))
				UNION ALL
				SELECT 1 FROM GFCADASTRO.TMP_ST_04 WHERE UPPER(TRIM(SERVTL_COD))= TRIM(UPPER(v_serv_cod))
			);
		EXCEPTION
		WHEN OTHERS THEN
			v_fl_exists := 0;
		END;	 
		
		IF v_fl_exists > 0 THEN
			p_inf.update_reg         := 1;
			p_inf.var05              := substr('prcts_regra_24u:' || p_inf.INFST_VAL_RED || p_inf.infst_isenta_icms || '|' || p_inf.infst_outras_icms || '|' || p_inf.estb_cod || '|' || p_inf.infst_tribicms || '|' || p_inf.infst_aliq_icms || '|' || p_inf.cfop || '>>' ||p_inf.VAR05,1,150) ;	 			
			p_inf.INFST_OUTRAS_ICMS  :=  NVL(p_inf.INFST_OUTRAS_ICMS,0) + NVL(p_inf.INFST_ISENTA_ICMS,0);
			p_inf.INFST_ISENTA_ICMS  := 0;
			p_inf.INFST_VAL_RED      := 0;
			p_inf.INFST_ALIQ_ICMS    := 0;
			p_inf.ESTB_COD           := '90';
			p_inf.INFST_TRIBICMS     := 'P';
			p_inf.CFOP               := '0000';
			v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
	  									p_nm_var01      =>  p_inf.emps_cod,
	  									p_nm_var02      =>  p_inf.fili_cod,
	  									p_nm_var03      =>  p_inf.infst_serie,
	  									p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
	  									p_nm_var05      =>  '|R2015_24|',
	  									p_nr_var02      =>  1);				
	  END IF;

	END IF;

END;