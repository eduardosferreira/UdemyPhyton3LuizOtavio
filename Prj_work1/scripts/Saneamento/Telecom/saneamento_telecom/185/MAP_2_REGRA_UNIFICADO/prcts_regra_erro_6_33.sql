-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_6_33(p_nf          IN OUT NOCOPY c_nf%rowtype)
AS
	v_fase_num_fat     openrisow.itfa.fase_num_fat%type := NULL;
	v_fase_num_fat_aux openrisow.itfa.fase_num_fat%type := NULL;
BEGIN
  


	IF p_nf.tipo_utilizacao IS NOT NULL AND NVL(p_nf.tipo_utilizacao, '--') <> NVL(p_nf.mnfst_tip_util, '--') THEN 

			p_nf.update_reg        := 1;
			p_nf.var05             := SUBSTR('erro_6_33:' || p_nf.mnfst_tip_util || '>>' ||p_nf.var05,1,150);
			p_nf.mnfst_tip_util    := p_nf.tipo_utilizacao;	 	
			v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
											  p_nm_var01  =>  p_nf.emps_cod,
											  p_nm_var02  =>  p_nf.fili_cod,
											  p_nm_var03  =>  p_nf.mnfst_serie,
											  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
											  p_nm_var05  =>  '|ERRO_6_33|',
											  p_nr_var01  =>  1); 							
			
	END IF;
	


END;
--/	