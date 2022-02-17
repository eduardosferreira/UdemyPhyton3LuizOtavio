-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_itens_seq(p_inf         IN OUT NOCOPY c_inf%rowtype)
AS
  -- MAP_2_REGRA_1_AJUSTE_DESCONTO_SEM_BASE
BEGIN

	IF p_inf.infst_num_seq_aux IS NOT NULL THEN
	
		p_inf.update_reg         := 1;
		p_inf.var05              := SUBSTR('erro_itens_seq:' || p_inf.infst_num_seq || '>>' ||p_inf.var05,1,150);
		p_inf.infst_num_seq      := p_inf.infst_num_seq_aux;								
	    v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
									p_nm_var01      =>  p_inf.emps_cod,
									p_nm_var02      =>  p_inf.fili_cod,
									p_nm_var03      =>  p_inf.infst_serie,
									p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
									p_nm_var05      =>  '|ERRO_ITENS_SEQ|',
									p_nr_var02      =>  1);	 	
									
	END IF;

END;
--/