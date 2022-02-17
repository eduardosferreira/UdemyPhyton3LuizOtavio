-- CREATE OR REPLACE 
PROCEDURE prcts_regra_52(
		p_cli            IN OUT NOCOPY c_cli%ROWTYPE,
		p_inf            IN c_inf%rowtype)
AS
-- "Onde:
-- ITEM_NFTL_SERV.FILI_COD in('3506','3501','3516') --Apenas para filial de Pernambuco
-- (SUBSTR(ITEM_NFTL_SERV.CFOP,2,3) IN('301','302','303','304','305','306') AND COMPLVU_CLIFORNEC.CADG_TIP_ASSIN <> '1') OR (SUBSTR(ITEM_NFTL_SERV.CFOP,2,3) ='307' AND COMPLVU_CLIFORNEC.CADG_TIP_ASSIN <> '3')
-- 
-- Então:
-- Se SUBSTR(ITEM_NFTL_SERV.CFOP,2,3) IN('301','302','303','304','305','306')
-- COMPLVU_CLIFORNEC.CADG_TIP_ASSIN := '1'
-- 
-- Se SUBSTR(ITEM_NFTL_SERV.CFOP,2,3) ='307'
-- COMPLVU_CLIFORNEC.CADG_TIP_ASSIN := '3'"
BEGIN
  IF    p_inf.rowid_inf IS NOT NULL 
	AND p_inf.FILI_COD in('3506','3501','3516') --Apenas para filial de Pernambuco
	AND ((SUBSTR(p_inf.CFOP,2,3) IN('301','302','303','304','305','306') AND p_cli.CADG_TIP_ASSIN <> '1') 
	  OR (SUBSTR(p_inf.CFOP,2,3) = '307' AND p_cli.CADG_TIP_ASSIN <> '3'))
  THEN 
	IF SUBSTR(p_inf.CFOP,2,3) IN('301','302','303','304','305','306') 
	THEN
		p_cli.update_reg_comp     := 1;
		p_cli.var05_cli           := SUBSTR('prcts_regra_52<<1>>: ' || p_cli.cadg_tip_assin  || '>>' ||p_cli.var05_cli,1,150);
		p_cli.cadg_tip_assin      := '1';	
		v_cli_rules_comp         := fncts_add_var(p_ds_rules  =>  v_cli_rules_comp, 
												  p_nm_var01  =>  p_inf.emps_cod,
						                          p_nm_var02  =>  p_inf.fili_cod,
						                          p_nm_var03  =>  p_inf.infst_serie,
												  p_nm_var04  =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R2015_52|',
												  p_nr_var04  =>  1); 			

	ELSIF SUBSTR(p_inf.CFOP,2,3) ='307' THEN
		p_cli.update_reg_comp     := 1;
		p_cli.var05_cli           := SUBSTR('prcts_regra_52<<2>>: ' || p_cli.cadg_tip_assin  || '>>' ||p_cli.var05_cli,1,150);
		p_cli.cadg_tip_assin      := '3';
		v_cli_rules_comp         := fncts_add_var(p_ds_rules  =>  v_cli_rules_comp, 
												  p_nm_var01  =>  p_inf.emps_cod,
						                          p_nm_var02  =>  p_inf.fili_cod,
						                          p_nm_var03  =>  p_inf.infst_serie,
												  p_nm_var04  =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R2015_52|',
												  p_nr_var04  =>  1); 			
	END IF;
  END IF;
END;
--/