-- CREATE OR REPLACE 
-- CURSOR c_regra_57  
-- IS
-- SELECT * FROM openrisow.TB_DE_PARA_57;	
-- 
-- c_regra_57_i VARCHAR2(150);
-- TYPE c_regra_57_t IS TABLE OF c_regra_57%ROWTYPE INDEX BY c_regra_57_i%TYPE;
-- v_bk_regra_57     c_regra_57_t;

PROCEDURE prcts_regra_57
AS	
BEGIN
	FOR i IN c_regra_57 LOOP
		v_bk_regra_57(TRIM(i.CADG_TIP_CLI)|| '|' ||TRIM(i.CADG_TIP)) := i;
	END LOOP;
END;

PROCEDURE prcts_regra_57(
      p_cli          IN OUT NOCOPY c_cli%ROWTYPE
	, p_inf          IN c_inf%ROWTYPE  
	)
AS
	v_nr_add_comp    NUMBER := 0;
	v_ddo_regra_57   c_regra_57%ROWTYPE;	
	
BEGIN

IF  p_inf.fili_cod NOT IN ('3506','3501')
THEN
	RETURN;
END IF;

	IF p_cli.rowid_comp IS NOT NULL
	THEN
	
		BEGIN	
	
			v_ddo_regra_57 := v_bk_regra_57(TRIM(p_cli.CADG_TIP_CLI)|| '|' ||TRIM(p_cli.CADG_TIP));
						
			IF  (nvl(p_cli.CADG_TIP_ASSIN,'0') != nvl(v_ddo_regra_57.CADG_TIP_ASSIN,'0'))
			AND (v_ddo_regra_57.CADG_TIP_CLI IS NOT NULL AND v_ddo_regra_57.CADG_TIP IS NOT NULL) 
			THEN
	
				v_nr_add_comp			 := 1;
				p_cli.update_reg_comp    := 1;	
				p_cli.var05_comp         := SUBSTR('r2015_57u:' || p_cli.CADG_TIP_ASSIN || '>>' ||p_cli.var05_comp,1,150);
				p_cli.cadg_tip_assin     := v_ddo_regra_57.CADG_TIP_ASSIN;

				v_cli_rules_comp      	 :=	 fncts_add_var(p_ds_rules      =>  v_inf_rules
															, p_nm_var01      =>  p_inf.emps_cod
															, p_nm_var02      =>  p_inf.fili_cod
															, p_nm_var03      =>  p_inf.infst_serie
															, p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD')
															, p_nm_var05      =>  '|R2015_57|'
															, p_nr_var04      =>  v_nr_add_comp
															);	  	

			END IF;	
			
		EXCEPTION
			WHEN OTHERS THEN
				NULL;					
		END;
		
	END IF;


END;
--/