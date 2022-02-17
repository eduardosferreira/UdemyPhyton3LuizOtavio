-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_122(
    p_f              IN c_f%ROWTYPE,
    p_nf             IN OUT NOCOPY c_nf%rowtype)
AS
    v_mnfst_codh_nf  openrisow.mestre_nftl_serv.mnfst_codh_nf%type := p_nf.mnfst_codh_nf;
BEGIN



	IF p_nf.mnfst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') THEN
		-- old v_mnfst_codh_nf := LOWER(RAWTOHEX(DBMS_OBFUSCATION_TOOLKIT.MD5(INPUT => UTL_RAW.CAST_TO_RAW(LPAD(p_nf.cnpj_cpf, 14, 0) || LPAD(p_nf.mnfst_num, 9, 0)|| LPAD(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_tot, '099999999.99'), '.', ''), ',', '')), 12, 0)|| LPAD(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_basicms, '099999999.99'), '.', ''), ',', '')), 12, 0) || LPAD(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_icms, '099999999.99'), '.', ''), ',', '')), 12, 0)))));
		       v_mnfst_codh_nf :=       rawtohex(dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw(lpad(p_nf.cnpj_cpf, 14, 0) || lpad(p_nf.mnfst_num, 9, 0)|| lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_tot, '099999999.99'), '.', ''), ',', '')), 12, 0)|| lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_basicms, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_icms, '099999999.99'), '.', ''), ',', '')), 12, 0))));

	ELSE
		-- old v_mnfst_codh_nf := LOWER(RAWTOHEX(DBMS_OBFUSCATION_TOOLKIT.MD5(INPUT => UTL_RAW.CAST_TO_RAW(LPAD(p_nf.cnpj_cpf, 14, 0) || LPAD(p_nf.MNFST_NUM, 9, 0)|| LPAD(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_tot, '9999999999.99'), '.', ''), ',', '')), 12, 0)|| LPAD(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_basicms, '9999999999.99'), '.', ''), ',', '')), 12, 0)|| LPAD(TRIM(REPLACE(REPLACE(TO_CHAR(p_nf.mnfst_val_icms, '9999999999.99'), '.', ''), ',', '')), 12, 0)	|| TO_CHAR(p_nf.mnfst_dtemiss, 'YYYYMMDD')	|| LPAD(p_f.fili_cod_cgc, 14, 0)))));	
			   v_mnfst_codh_nf :=       rawtohex(dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw(lpad(p_nf.cnpj_cpf, 14, 0) || lpad(p_nf.mnfst_num, 9, 0)|| lpad(trim(replace(replace(to_char(p_nf.mnfst_val_tot, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_basicms, '099999999.99'), '.', ''), ',', '')), 12, 0) || lpad(trim(replace(replace(to_char(p_nf.mnfst_val_icms, '099999999.99'), '.', ''), ',', '')), 12, 0)||to_char(p_nf.mnfst_dtemiss, 'yyyymmdd')      || lpad(p_f.fili_cod_cgc, 14, 0))));
	
	END IF;
		
	
	IF 	((p_nf.mnfst_codh_nf IS NULL) OR	(NVL(p_nf.mnfst_codh_nf,' ')          != v_mnfst_codh_nf))
	THEN
		p_nf.var05              := SUBSTR('erro_122:' || p_nf.mnfst_codh_nf || '>>' ||p_nf.var05,1,150);
		p_nf.update_reg         := 1;
		p_nf.mnfst_codh_nf        := v_mnfst_codh_nf;
		v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
										  p_nm_var01  =>  p_nf.emps_cod,
										  p_nm_var02  =>  p_nf.fili_cod,
										  p_nm_var03  =>  p_nf.mnfst_serie,
										  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
										  p_nm_var05  =>  '|ERRO_122|',
										  p_nr_var01  =>  1); 							
	END IF;	

    

  
END;
-- /
