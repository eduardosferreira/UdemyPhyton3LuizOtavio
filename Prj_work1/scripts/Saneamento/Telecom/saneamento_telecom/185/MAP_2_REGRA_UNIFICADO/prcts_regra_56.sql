-- CREATE OR REPLACE 
PROCEDURE prcts_regra_56(
      p_cli          IN OUT NOCOPY c_cli%ROWTYPE
	, p_inf          IN OUT NOCOPY c_inf%ROWTYPE	
	)
AS
	v_nr_add_inf     NUMBER := 0;
	v_nr_add_cli     NUMBER := 0;
	v_nr_add_comp    NUMBER := 0;
    v_cfop           openrisow.item_nftl_serv.cfop%type 			    := p_inf.cfop;
	v_cadg_tip_assin openrisow.complvu_clifornec.cadg_tip_assin%type    := p_cli.cadg_tip_assin;
BEGIN

	-- Regra 56) Grupos Inválidos de Pernambuco
	-- Rodar apenas para filiais '3506','3501'
    
	IF ( p_inf.fili_cod IN ('3506','3501'))
	THEN

		IF (
			/*Caso 1 */
				v_cadg_tip_assin 				 	 = 1
			AND NVL (p_cli.CADG_COD_INSEST,'ISENTO') = 'ISENTO'
			AND p_cli.CADG_TIP						 = 'F'
			AND SUBSTR(v_cfop,2,3) 					 IN ('301','302','303','304','305','306','307')
		) THEN
		
			v_nr_add_inf			 := 1;
			p_inf.update_reg         := 1;	
			p_inf.var05              := SUBSTR('r2015_56u<1>:' || v_cfop || '>>' ||p_inf.var05,1,150);
			v_cfop 					 := SUBSTR(v_cfop,1,1) || '307';
			p_inf.CFOP               := v_cfop;
		
			v_nr_add_comp			 := 1;
			p_cli.update_reg_comp    := 1;	
			p_cli.var05_comp         := SUBSTR('r2015_56u<1>:' || v_cadg_tip_assin || '>>' ||p_cli.var05_comp,1,150);
			v_cadg_tip_assin         := 3;
			p_cli.cadg_tip_assin     := v_cadg_tip_assin;
		
		END IF;
	 

		IF (
				/*Caso 2 */
				v_cadg_tip_assin = 1
			AND NVL (p_cli.CADG_COD_INSEST,'ISENTO') != 'ISENTO'
			AND p_cli.CADG_TIP = 'F'
			AND SUBSTR(v_cfop,2,3) IN ('301','305','307')
		) THEN
		
			v_nr_add_inf			 := 1;
			p_inf.update_reg         := 1;	
			p_inf.var05              := SUBSTR('r2015_56u<2>:' || v_cfop || '>>' ||p_inf.var05,1,150);
			v_cfop 					 := SUBSTR(v_cfop,1,1) || '303';
			p_inf.CFOP               := v_cfop;
	
		END IF;
		
	
		
	
		IF (
				/* Caso 3 */
				v_cadg_tip_assin   = 1
			AND p_cli.CADG_TIP 	   = 'J'
			AND SUBSTR(v_cfop,2,3) = '307'	
		) THEN
		
			v_nr_add_inf			 := 1;
			p_inf.update_reg         := 1;	
			p_inf.var05              := SUBSTR('r2015_56u<3>:' || v_cfop || '>>' ||p_inf.var05,1,150);
			v_cfop 					 := SUBSTR(v_cfop,1,1) || '303';
			p_inf.CFOP               := v_cfop;
	
		END IF; 	 
	
		
	
		IF (
				/*Caso 4 */
				v_cadg_tip_assin 					  = 3
			AND NVL (p_cli.CADG_COD_INSEST,'ISENTO')  = 'ISENTO'
			AND SUBSTR(v_cfop,2,3) IN ('301','302','303','304','305','306')
		) THEN
		
			v_nr_add_inf			 := 1;
			p_inf.update_reg         := 1;	
			p_inf.var05              := SUBSTR('r2015_56u<4>:' || v_cfop || '>>' ||p_inf.var05,1,150);
			v_cfop 					 := SUBSTR(v_cfop,1,1) || '307';
			p_inf.CFOP               := v_cfop;
	
		END IF; 
	
		
	
		IF (
				/*Caso 5 */
				v_cadg_tip_assin 					  = 3
			AND NVL (p_cli.CADG_COD_INSEST,'ISENTO') != 'ISENTO'
			AND (SUBSTR(v_cfop,2,3) IN ('302','303','304','306') or (SUBSTR(v_cfop,2,3) IN ('301','305') and p_cli.CADG_TIP = 'J'))
		) THEN
		
			v_nr_add_comp			 := 1;
			p_cli.update_reg_comp    := 1;	
			p_cli.var05_comp         := SUBSTR('r2015_56u<5>:' || v_cadg_tip_assin || '>>' ||p_cli.var05_comp,1,150);
			v_cadg_tip_assin         := 1;
			p_cli.cadg_tip_assin     := v_cadg_tip_assin;
		
		END IF;	 
	
		
	
		
	
		IF (
				/*Caso 6 */
				v_cadg_tip_assin 				      = 3
			AND NVL (p_cli.CADG_COD_INSEST,'ISENTO') != 'ISENTO'
			AND ((p_cli.CADG_TIP = 'F' and SUBSTR(v_cfop,2,3) IN ('301','305','307')) or (SUBSTR(v_cfop,2,3) IN ('307') and p_cli.CADG_TIP = 'J'))
		) THEN
		
			v_nr_add_inf			 := 1;
			p_inf.update_reg         := 1;	
			p_inf.var05              := SUBSTR('r2015_56u<6>:' || v_cfop || '>>' ||p_inf.var05,1,150);
			v_cfop 					 := SUBSTR(v_cfop,1,1) || '303';
			p_inf.CFOP               := v_cfop;
		
			v_nr_add_comp			 := 1;
			p_cli.update_reg_comp    := 1;	
			p_cli.var05_comp         := SUBSTR('r2015_56u<6>:' || v_cadg_tip_assin || '>>' ||p_cli.var05_comp,1,150);
			v_cadg_tip_assin         := 1;
			p_cli.cadg_tip_assin     := v_cadg_tip_assin;
		
		END IF;
		
	
		IF (
				/*Caso 7 */
				v_cadg_tip_assin 				 	 = 6
			AND NVL (p_cli.CADG_COD_INSEST,'ISENTO') = 'ISENTO'
			AND p_cli.CADG_TIP 					 = 'F'
			AND SUBSTR(v_cfop,2,3) 					 IN ('301','302','303','304','305','306')
		) THEN
		
			v_nr_add_inf			 := 1;
			p_inf.update_reg         := 1;	
			p_inf.var05              := SUBSTR('r2015_56u<7>:' || v_cfop || '>>' ||p_inf.var05,1,150);
			v_cfop 					 := SUBSTR(v_cfop,1,1) || '307';
			p_inf.CFOP               := v_cfop;
		
		END IF;
		
	
		IF (
				/*Caso 8 */
				v_cadg_tip_assin 				      = 6
			AND	NVL (p_cli.CADG_COD_INSEST,'ISENTO') != 'ISENTO'
			AND SUBSTR(v_cfop,2,3) 					  = '307'
		) THEN
		
			v_nr_add_inf			 := 1;
			p_inf.update_reg         := 1;	
			p_inf.var05              := SUBSTR('r2015_56u<8>:' || v_cfop || '>>' ||p_inf.var05,1,150);
			v_cfop 					 := SUBSTR(v_cfop,1,1) || '303';
			p_inf.CFOP               := v_cfop;
		
		END IF;
		
		
		IF (v_nr_add_inf  > 0 AND NVL(p_inf.update_reg,0) 	   != 0 AND p_inf.rowid_inf  IS NOT NULL)
		THEN
			v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules
												, p_nm_var01      =>  p_inf.emps_cod
												, p_nm_var02      =>  p_inf.fili_cod
												, p_nm_var03      =>  p_inf.infst_serie
												, p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD')
												, p_nm_var05      =>  '|R2015_56|'
												, p_nr_var02      =>  v_nr_add_inf
												);	  	
				
		END IF;	
	
	
		IF (v_nr_add_comp > 0 AND NVL(p_cli.update_reg_comp,0) != 0 AND p_cli.rowid_comp IS NOT NULL)
		THEN
			v_cli_rules_comp      := fncts_add_var(p_ds_rules      =>  v_inf_rules
												, p_nm_var01      =>  p_inf.emps_cod
												, p_nm_var02      =>  p_inf.fili_cod
												, p_nm_var03      =>  p_inf.infst_serie
												, p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD')
												, p_nm_var05      =>  '|R2015_56|'
												, p_nr_var04      =>  v_nr_add_comp
												);	  	
				
		END IF;
		
	END IF;	

END;
--/