-- CREATE OR REPLACE 
PROCEDURE prcts_regra_32(p_inf         IN OUT NOCOPY c_inf%rowtype, p_sanea  IN  c_sanea%rowtype)
AS
  v_fl_exists 			PLS_INTEGER := 0;
  v_serv_cod            openrisow.item_nftl_serv.serv_cod%type; 
BEGIN
  IF (p_inf.CFOP = '0000' AND p_inf.rowid_inf IS NOT NULL )
  THEN
	  v_serv_cod       := TRIM(SUBSTR(fccts_retira_caracter(p_ds_ddo => p_inf.serv_cod, p_ds_serie => p_inf.serie),1,60));  
	  BEGIN
	    SELECT /*+ first_rows(1)*/ NVL(COUNT(1),0) INTO v_fl_exists FROM DUAL
		WHERE EXISTS (
			SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD))= TRIM(UPPER(p_inf.serv_cod))
			UNION ALL
			SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD_ORIGINAL))= TRIM(UPPER(p_inf.serv_cod))
			UNION ALL
			SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD))= TRIM(UPPER(v_serv_cod))
			UNION ALL
			SELECT 1 FROM GFCADASTRO.TMP_ST_01 WHERE UPPER(TRIM(SERV_COD_ORIGINAL))= TRIM(UPPER(v_serv_cod))
		);
	  EXCEPTION
		WHEN OTHERS THEN
			v_fl_exists := 0;
	  END;
	  IF v_fl_exists > 0 THEN
		  p_inf.update_reg        				:= 1;
		  p_inf.var05             				:= substr('prcts_regra_32u:' || p_inf.CFOP || '|' || p_inf.SERV_COD  || '>>' ||p_inf.var05,1,150);
		  -- Altera o registro original
          p_inf.CFOP                            := (CASE
														WHEN UPPER(TRIM(NVL(p_sanea.cfop_max,'0000'))) = '0000' THEN
														   CASE 
															   WHEN  NVL(LENGTH(NVL(TRIM(p_inf.CGC_CPF),'9999')),0) > 11 THEN -- PJ 
																-- Se cliente PJ CFOP = '5303'
																'5303'
															   ELSE
																-- Se cliente PF CFOP = '5307'
																'5307'
														   END
													    ELSE
														   UPPER(TRIM(NVL(p_sanea.cfop_max,'0000')))
													END);
		  p_inf.SERV_COD                        := v_serv_cod;
		  v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
									               p_nm_var01      =>  p_inf.emps_cod,
									               p_nm_var02      =>  p_inf.fili_cod,
									               p_nm_var03      =>  p_inf.infst_serie,
									               p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
									               p_nm_var05      =>  '|R2015_32|',
									               p_nr_var02      =>  1);			
		  
	  END IF;
  END IF;
END;
--/