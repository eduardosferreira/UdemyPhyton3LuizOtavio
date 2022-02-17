-- ;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_atualizacaoZ04(p_cli            IN OUT NOCOPY c_cli%ROWTYPE, 
									 p_inf            IN OUT NOCOPY c_inf%rowtype, 
									 p_nf             IN OUT NOCOPY c_nf%rowtype)
AS
  v_cadg_cod_aux openrisow.cli_fornec_transp.cadg_cod%type;
  v_nr_add_nf    NUMBER := 0;
  v_nr_add_inf   NUMBER := 0;
  v_nr_add_cli   NUMBER := 0;
  v_nr_add_comp  NUMBER := 0;    
BEGIN
  --  1 – Levantar todos os cadastros da séries Z04 para o periodo de referencia. 
  IF ( p_nf.serie IN ('Z04') ) 
  THEN 
-- Duplicar o cadastro, mudando apenas o CADG_COD, conforme :   XXX000000000AAMM, onde:
-- XXX = série sem espaos (no caso Z04)
-- 000000000 = Número da NF (eexemplo “000000231”)
-- AAMM = Ano (dois digitos) e Mes de Emissão
	-- <cadg_cod_cli>
	v_cadg_cod_aux  := TRIM(SUBSTR(TRIM(SUBSTR(TRIM(p_nf.serie),1,3)) || LPAD(SUBSTR(TRIM(p_nf.mnfst_num),1,9),9,'0') || TO_CHAR(v_nf.mnfst_dtemiss,'YYMM'),1,16));
	IF TRIM(TO_CHAR(v_cadg_cod_aux))         != TRIM(TO_CHAR(p_nf.cadg_cod))
	THEN
		v_nr_add_nf            := 1; 
		p_nf.update_reg        := 1;
		p_nf.var05             := SUBSTR('prcts_regra_atualizacaoZ04<cadg_cod>:' || p_nf.cadg_cod       || '>>' || p_nf.var05,1,150);
		p_nf.cadg_cod          := TRIM(TO_CHAR(v_cadg_cod_aux));		
	END IF;	
	IF TRIM(TO_CHAR(p_inf.cadg_cod))         != TRIM(TO_CHAR(p_nf.cadg_cod))
	THEN
		v_nr_add_inf           := 1;
		p_inf.update_reg       := 1;
		p_inf.var05            := SUBSTR('prcts_regra_atualizacaoZ04<cadg_cod>:' || p_inf.cadg_cod      || '>>' || p_inf.var05,1,150);
		p_inf.cadg_cod         := p_nf.cadg_cod;
	END IF;		
	IF TRIM(TO_CHAR(p_nf.cadg_cod))          != TRIM(TO_CHAR(p_cli.cadg_cod_new_cli))
	THEN
		v_nr_add_cli                 := 1;
		v_nr_add_comp                := 1;
		p_cli.insere_reg             := 1;
		p_cli.insere_reg_comp        := 1;
		p_cli.var05_cli_new          := SUBSTR('prcts_regra_atualizacaoZ04<cadg_cod>:' || p_cli.cadg_cod_new_cli || '>>' || p_cli.var05_cli_new,1,150);
		p_cli.cadg_cod_new_cli       := TRIM(TO_CHAR(p_nf.cadg_cod));
		p_cli.cadg_cod_new_comp      := p_cli.cadg_cod_new_cli;			
	END IF;	
	
	IF v_nr_add_nf    > 0
    THEN	
		v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R_ATUALIZA_Z04|',
												  p_nr_var01  =>  1);  
	END IF;
	
	IF v_nr_add_inf    > 0 AND NVL(p_inf.update_reg,0) != 0 AND p_inf.rowid_inf IS NOT NULL
    THEN	
		v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
												 p_nm_var01      =>  p_inf.emps_cod,
		                                         p_nm_var02      =>  p_inf.fili_cod,
		                                         p_nm_var03      =>  p_inf.infst_serie,
						                         p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
		                                         p_nm_var05      =>  '|R_ATUALIZA_Z04|',
						                         p_nr_var02      =>  1);
	END IF;
	
  END IF;

END;
--/	