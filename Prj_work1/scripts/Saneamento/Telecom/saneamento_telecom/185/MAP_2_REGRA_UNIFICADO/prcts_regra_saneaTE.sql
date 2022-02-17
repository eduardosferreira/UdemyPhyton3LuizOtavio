-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_saneaTE(p_cli            IN OUT NOCOPY c_cli%ROWTYPE, p_inf          IN OUT NOCOPY c_inf%rowtype, p_nf  IN OUT NOCOPY   c_nf%rowtype, p_sanea  IN  c_sanea%rowtype)
AS
   v_cadg_tip_cli   openrisow.complvu_clifornec.cadg_tip_cli%type   := '99';
   v_cadg_tip_assin openrisow.complvu_clifornec.cadg_tip_assin%type := '6';
   v_nr_add_nf    NUMBER := 0;
   v_nr_add_inf   NUMBER := 0;
   v_nr_add_cli   NUMBER := 0;
   v_nr_add_comp  NUMBER := 0;
BEGIN
  
  IF p_nf.serie = 'TE' 
  THEN
  
	IF (p_sanea.cfop_73 like '73%')  THEN    

		 -- <cadg_cod_cgccpf>
		 IF TRIM(TO_CHAR(p_cli.cadg_cod_cgccpf))   != '00000000000000' THEN
			v_nr_add_cli           := 1;
			p_cli.update_reg       := 1;
			p_cli.var05_cli        := SUBSTR('saneaTE<cadg_cod_cgccpf>:' || p_cli.cadg_cod_cgccpf  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_cod_cgccpf  := '00000000000000';
		 END IF;	 
		 IF TRIM(TO_CHAR(p_nf.cnpj_cpf)) !=  TRIM(TO_CHAR(p_cli.cadg_cod_cgccpf)) THEN
			v_nr_add_nf            := 1;
			p_nf.update_reg        := 1;
			p_nf.var05             := SUBSTR('saneaTE<cadg_cod_cgccpf>:' || p_nf.cnpj_cpf || '>>' || p_nf.var05,1,150);
			p_nf.cnpj_cpf          := TRIM(TO_CHAR(p_cli.cadg_cod_cgccpf));	 
		 END IF;	 
		 IF TRIM(TO_CHAR(p_inf.cgc_cpf)) !=  TRIM(TO_CHAR(p_nf.cnpj_cpf)) THEN
			v_nr_add_inf            := 1;
			p_inf.update_reg        := 1;
			p_inf.var05             := SUBSTR('saneaTE<cadg_cod_cgccpf>:' || p_inf.cgc_cpf || '>>' || p_inf.var05,1,150);
			p_inf.cgc_cpf           := TRIM(TO_CHAR(p_nf.cnpj_cpf));	 
		 END IF;
		 
		 -- <cadg_cod_insest>
		 IF p_cli.cadg_cod_insest   != 'ISENTO' THEN
		    v_nr_add_cli           := 1;
			p_cli.update_reg       := 1;
			p_cli.var05_cli        := SUBSTR('saneaTE<cadg_cod_insest>:' || p_cli.cadg_cod_insest  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_cod_insest  := 'ISENTO';
		 END IF;

		 -- <cadg_tip>
		 IF UPPER(TRIM(TO_CHAR(p_cli.cadg_tip)))   	   != 'E' THEN
		    v_nr_add_cli           := 1;
			p_cli.update_reg       := 1;
			p_cli.var05_cli        := SUBSTR('saneaTE<CADG_TIP>:' || p_cli.cadg_tip  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_tip         := 'E';
		 END IF;
		 
		 -- <unfe_sig>
		 IF TRIM(TO_CHAR(p_cli.unfe_sig_cli))   != 'EX' THEN
		    v_nr_add_cli           := 1;
			p_cli.update_reg       := 1;
			p_cli.var05_cli        := SUBSTR('saneaTE<unfe_sig>:' || p_cli.unfe_sig_cli  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.unfe_sig_cli     := 'EX';
		 END IF;		 
		 IF TRIM(TO_CHAR(p_inf.uf)) !=  TRIM(TO_CHAR(p_cli.unfe_sig_cli)) THEN
			v_nr_add_inf            := 1;
			p_inf.update_reg        := 1;
			p_inf.var05             := SUBSTR('saneaTE<unfe_sig>:' || p_inf.uf || '>>' || p_inf.var05,1,150);
			p_inf.uf                := TRIM(TO_CHAR(p_cli.unfe_sig_cli));	 
		 END IF;
		 IF TRIM(TO_CHAR(p_cli.cadg_uf_habilit)) != TRIM(TO_CHAR(p_cli.unfe_sig_cli)) THEN
			v_nr_add_comp             := 1;
			p_cli.update_reg_comp     := 1;
			p_cli.var05_cli           := SUBSTR('saneaTE<unfe_sig_cli>' || p_cli.cadg_uf_habilit || '>>' ||p_cli.var05_cli,1,150);
			p_cli.cadg_uf_habilit     := TRIM(TO_CHAR(p_cli.unfe_sig_cli));
		 END IF;	 

		 -- <tip_cli|tip_assin>
		 IF v_cadg_tip_cli != NVL(p_cli.cadg_tip_cli,'X') OR v_cadg_tip_assin != NVL(p_cli.cadg_tip_assin,'X')  THEN
			v_nr_add_comp             := 1;
			p_cli.update_reg_comp     := 1;
			p_cli.var05_cli           := SUBSTR('saneaTE<tip_cli|tip_assin>' || p_cli.cadg_tip_assin || '|' || p_cli.cadg_tip_cli  || '>>' ||p_cli.var05_cli,1,150);
			p_cli.cadg_tip_assin      := v_cadg_tip_assin;
			p_cli.cadg_tip_cli        := v_cadg_tip_cli; 
		 END IF;

		 -- <cadg_num_conta>
		 IF p_cli.cadg_num_conta != ' '  THEN
			v_nr_add_comp             := 1;
			p_cli.update_reg_comp     := 1;
			p_cli.var05_cli           := SUBSTR('saneaTE<cadg_num_conta>' || p_cli.cadg_num_conta || '>>' ||p_cli.var05_cli,1,150);
			p_cli.cadg_num_conta      :=  ' ' ;
		 END IF;

		 -- <cadg_end_munic>
		 IF UPPER(TRIM(TO_CHAR(p_cli.cadg_end_munic_cli)))   != 'EXTERIOR' THEN
			v_nr_add_cli               := 1;
			p_cli.update_reg           := 1;
			p_cli.var05_cli            := SUBSTR('saneaTE<cadg_end_munic>:' || p_cli.cadg_end_munic_cli  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_end_munic_cli   := 'Exterior';
		 END IF;	

		 -- <mibge_cod_mun>
		 IF TRIM(TO_CHAR(p_cli.mibge_cod_mun_cli))   != '9999999' THEN
			v_nr_add_cli              := 1;
			p_cli.update_reg          := 1;
			p_cli.var05_cli           := SUBSTR('saneaTE<mibge_cod_mun>:' || p_cli.mibge_cod_mun_cli  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.mibge_cod_mun_cli   := '9999999';
		 END IF;	

		 -- <cadg_end_cep>
		 IF TRIM(TO_CHAR(p_cli.cadg_end_cep))   != '0' THEN
			v_nr_add_cli              := 1;
			p_cli.update_reg          := 1;
			p_cli.var05_cli           := SUBSTR('saneaTE<cadg_end_cep>:' || p_cli.cadg_end_cep  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_end_cep        := '0';
		 END IF;	
		 
		 -- <cadg_end_bairro>
		 IF p_cli.cadg_end_bairro     != ' ' THEN
			v_nr_add_cli              := 1;
			p_cli.update_reg          := 1;
			p_cli.var05_cli           := SUBSTR('saneaTE<cadg_end_bairro>:' || p_cli.cadg_end_bairro  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_end_bairro     := ' ';
		 END IF;		 

		 -- <cadg_tel_contato>
	   
	   IF p_nf.mnfst_dtemiss <= to_date('31/12/2016','dd/mm/yyyy') THEN
	   
	       IF nvl(p_cli.cadg_tel_contato,'X') !=  '1100000000'   THEN
		      	v_nr_add_comp             := 1;
				p_cli.update_reg_comp     := 1;
			    p_cli.var05_cli           := SUBSTR('saneaTE<cadg_tel_contato>' || p_cli.cadg_tel_contato || '>>' ||p_cli.var05_cli,1,150);
      			p_cli.cadg_tel_contato    :=  '1100000000' ;
    		 END IF;
     ELSE
         IF TRIM(TO_CHAR(p_cli.cadg_tel_contato)) !=  ' '   THEN
			    v_nr_add_comp             := 1;
				p_cli.update_reg_comp     := 1;
	      		p_cli.var05_cli           := SUBSTR('saneaTE<cadg_tel_contato>' || p_cli.cadg_tel_contato || '>>' ||p_cli.var05_cli,1,150);
		       	p_cli.cadg_tel_contato    :=  ' ' ;
       	 END IF;

	  END IF;
		 
	ELSIF (p_sanea.cfop_73 like '%301') 
	  OR  (p_sanea.cfop_73 like '%305') 
	  OR  (p_sanea.cfop_73 like '%306') 
	THEN 
		 IF UPPER(TRIM(TO_CHAR(p_cli.cadg_tip)))      	   != 'J' THEN
			v_nr_add_cli           := 1;
			p_cli.update_reg       := 1;
			p_cli.var05_cli        := SUBSTR('saneaTE<CADG_TIP-J>:' || p_cli.cadg_tip  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_tip         := 'J';
		 END IF;		
	ELSIF (p_sanea.cfop_73 like '%307') 
	THEN
		 IF UPPER(TRIM(TO_CHAR(p_cli.cadg_tip)))      	   != 'F' THEN
			v_nr_add_cli           := 1;
			p_cli.update_reg       := 1;
			p_cli.var05_cli        := SUBSTR('saneaTE<CADG_TIP-F>:' || p_cli.cadg_tip  || '>>' ||p_cli.var05_cli,1,150);	 
			p_cli.cadg_tip         := 'F';
		 END IF;		
	END IF;
		
	IF v_nr_add_nf    > 0
    THEN	
		v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R_SANEA_TE|',
												  p_nr_var01  =>  1);  
	END IF;
	
	IF v_nr_add_inf    > 0 AND NVL(p_inf.update_reg,0) != 0 AND p_inf.rowid_inf IS NOT NULL
    THEN	
		v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
												 p_nm_var01      =>  p_inf.emps_cod,
		                                         p_nm_var02      =>  p_inf.fili_cod,
		                                         p_nm_var03      =>  p_inf.infst_serie,
						                         p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
		                                         p_nm_var05      =>  '|R_SANEA_TE|',
						                         p_nr_var02      =>  1);
	END IF;
	
	IF v_nr_add_cli    > 0
    THEN	
		v_cli_rules              := fncts_add_var(p_ds_rules  =>  v_cli_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R_SANEA_TE|',
												  p_nr_var03  =>  1); 		
	END IF;
	
	IF v_nr_add_comp    > 0
    THEN	
		v_cli_rules_comp         := fncts_add_var(p_ds_rules  =>  v_cli_rules_comp, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R_SANEA_TE|',
												  p_nr_var04  =>  1); 		

	END IF;	
	
  END IF;

END;
--/	