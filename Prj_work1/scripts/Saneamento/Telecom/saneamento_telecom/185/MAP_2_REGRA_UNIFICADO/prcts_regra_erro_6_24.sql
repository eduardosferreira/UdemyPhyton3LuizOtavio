-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_6_24(
	p_inf            IN  c_inf%rowtype,
    p_nf             IN  c_nf%rowtype,
	p_cli            IN OUT NOCOPY c_cli%ROWTYPE)
AS
	v_cadg_tip_assin openrisow.complvu_clifornec.cadg_tip_assin%type := p_cli.cadg_tip_assin ;
BEGIN

	IF (p_nf.mnfst_dtemiss >= TO_DATE('2017-01-01', 'YYYY-MM-DD') AND NVL(TRIM(p_cli.cadg_tip_assin), ' ') <> '0' ) 
	   OR (p_nf.mnfst_dtemiss < TO_DATE('2017-01-01', 'YYYY-MM-DD') AND  NVL(TRIM(p_cli.cadg_tip_assin), ' ') NOT IN ('1', '2', '3', '4', '5', '6')) 
	THEN
		IF p_inf.CFOP IN ('5301', '5302', '5303', '5304', '5305', '5306', '5307', '6301', '6302', '6303', '6304', '6305', '6306', '6307') THEN
			v_cadg_tip_assin    :=	(CASE 
										WHEN p_cli.cadg_tip = 'J' AND (p_inf.CFOP LIKE '_302' OR p_inf.CFOP LIKE '_303') THEN '1'
										WHEN p_cli.cadg_tip = 'F' AND p_inf.CFOP = '5307' THEN '3'
										WHEN p_cli.cadg_tip = 'J' AND p_inf.CFOP = '5306' THEN '4'
										WHEN p_cli.cadg_tip = 'J' AND p_inf.CFOP IN ('6301', '6304', '6305', '6306') THEN '6'
										WHEN p_cli.cadg_tip = 'F' THEN '3'
										WHEN p_cli.cadg_tip = 'J' THEN '1'
										ELSE p_cli.cadg_tip_assin 
									END); 
									
			IF p_cli.cadg_tip_assin != v_cadg_tip_assin THEN
				p_cli.update_reg_comp := 1;						
				p_cli.cadg_tip_assin  := v_cadg_tip_assin;
				v_cli_rules_comp      := fncts_add_var(p_ds_rules  =>  v_cli_rules_comp, 
													   p_nm_var01  =>  p_nf.emps_cod,
													   p_nm_var02  =>  p_nf.fili_cod,
													   p_nm_var03  =>  p_nf.mnfst_serie,
													   p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
													   p_nm_var05  =>  '|ERRO_6_24|',
													   p_nr_var04  =>  1); 								
			END IF;					
		END IF;
	END IF;
		
	

END;
--/	