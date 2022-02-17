-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_6_20(p_nf          IN OUT NOCOPY c_nf%rowtype)
AS
	v_fase_num_fat     openrisow.itfa.fase_num_fat%type := NULL;
	v_fase_num_fat_aux openrisow.itfa.fase_num_fat%type := NULL;
BEGIN
  
	IF p_nf.mnfst_serie NOT IN ('4', '5', '6', '8', 'C 2', 'C 3', 'C 5', 'C 9', 'C 10', 'C 14', '11', 'RP')  THEN 
    
		BEGIN
			SELECT it.fase_num_fat INTO v_fase_num_fat
			FROM  openrisow.itfa it
			WHERE it.emps_cod    = p_nf.emps_cod
			AND it.fili_cod      = p_nf.fili_cod
			AND it.mnfst_serie   = p_nf.mnfst_serie
			AND it.mnfst_num     = p_nf.mnfst_num
			AND it.mnfst_dtemiss = p_nf.mnfst_dtemiss
			AND it.mdoc_cod      = p_nf.mdoc_cod
			AND ROWNUM <= 1;
		EXCEPTION 
		 WHEN NO_DATA_FOUND THEN
			v_fase_num_fat      := p_nf.mnfst_num_fat;
			v_fase_num_fat_aux  := v_fase_num_fat;
		END;

		IF v_fase_num_fat LIKE '% %' OR v_fase_num_fat LIKE '%0%' OR v_fase_num_fat IS NULL	OR REGEXP_LIKE(v_fase_num_fat,'[^0-9]')	OR v_fase_num_fat = p_nf.mnfst_num	THEN 
			v_fase_num_fat := NULL;
		END IF;

		IF NVL(v_fase_num_fat_aux,'_xXx_') != NVL(v_fase_num_fat,'_xXx_') THEN
			p_nf.update_reg        := 1;
			p_nf.var05             := SUBSTR('erro_6_20:' || p_nf.mnfst_num_fat || '>>' ||p_nf.var05,1,150);
			p_nf.mnfst_num_fat     := v_fase_num_fat;	 	
			
			v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
													  p_nm_var01  =>  p_nf.emps_cod,
													  p_nm_var02  =>  p_nf.fili_cod,
													  p_nm_var03  =>  p_nf.mnfst_serie,
													  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
													  p_nm_var05  =>  '|ERRO_6_20|',
													  p_nr_var01  =>  1); 										
									
		END IF;

	END IF;	

END;
--/	