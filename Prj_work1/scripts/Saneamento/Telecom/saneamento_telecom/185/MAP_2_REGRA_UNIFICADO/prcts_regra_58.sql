PROCEDURE prcts_regra_58(p_inf          IN OUT NOCOPY c_inf%rowtype, p_nf  IN c_nf%rowtype)
AS

BEGIN
  
	IF p_nf.cadg_cod != p_inf.cadg_cod THEN

		INSERT INTO OPENRISOW.TB_DIV_CLI_MESTRE_ITEM(emps_cod               
													, fili_cod               
													, mnfst_serie            
													, mnfst_num              
													, mnfst_dtemiss          
													, mdoc_cod               
													, catg_cod_mestre        
													, cadg_cod_mestre        
													, catg_cod_item          
													, cadg_cod_item          
													, rowid_mestre           
													, rowid_item) VALUES (p_nf.emps_cod               
																		, p_nf.fili_cod               
																		, p_nf.mnfst_serie            
																		, p_nf.mnfst_num              
																		, p_nf.mnfst_dtemiss          
																		, p_nf.mdoc_cod               
																		, p_nf.catg_cod        
																		, p_nf.cadg_cod        
																		, p_inf.catg_cod          
																		, p_inf.cadg_cod          
																		, p_nf.rowid_nf           
																		, p_inf.rowid_inf             
																		); 		
		p_inf.update_reg        := 1;
		p_inf.var05             := SUBSTR('r2015_58u:' || p_inf.cadg_cod || '>>' || p_inf.var05,1,150);
		p_inf.cadg_cod          := p_nf.cadg_cod;	
	    v_inf_rules      		:= fncts_add_var(p_ds_rules      =>  v_inf_rules, 
												 p_nm_var01      =>  p_inf.emps_cod,
												 p_nm_var02      =>  p_inf.fili_cod,
												 p_nm_var03      =>  p_inf.infst_serie,
												 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
												 p_nm_var05      =>  '|R2015_58|',
												 p_nr_var02      =>  1);	
		
										
	
	
	END IF;
	


END;
--/	