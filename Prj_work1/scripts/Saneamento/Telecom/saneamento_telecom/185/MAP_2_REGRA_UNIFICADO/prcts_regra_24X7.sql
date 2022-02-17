-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_24X7(
	p_inf            IN OUT NOCOPY c_inf%rowtype,
    p_nf             IN OUT NOCOPY c_nf%rowtype,
	p_cli            IN OUT NOCOPY c_cli%ROWTYPE,
	p_sanea          IN c_sanea%ROWTYPE,
	p_nr_qtde_inf    IN PLS_INTEGER)
AS
-- MAP_2_24X7	
   v_cpf_cnpj       openrisow.mestre_nftl_serv.cnpj_cpf%type := null;
   v_cadg_tip_cli   openrisow.complvu_clifornec.cadg_tip_cli%type := null;
   v_cadg_tip_assin openrisow.complvu_clifornec.cadg_tip_assin%type := null;
   v_ie             openrisow.item_nftl_serv.ie%type := null;
   v_num_terminal   openrisow.item_nftl_serv.infst_tel%type := null;
   v_nr_add_nf      NUMBER := 0;
   v_nr_add_inf     NUMBER := 0;
   v_nr_add_cli     NUMBER := 0;
   v_nr_add_comp    NUMBER := 0;     
BEGIN

IF  p_inf.fili_cod IN ('3506','3501')
THEN
	RETURN;
END IF;

	IF (NVL(p_cli.cadg_num_conta,'1135497777') = '1135497777' OR p_cli.cadg_num_conta LIKE '%00000') AND TRIM(p_inf.infst_tel) IS NOT NULL AND p_inf.infst_tel NOT LIKE '%00000' THEN
		IF LENGTH(p_inf.infst_tel) NOT IN (10,11) OR
			  SUBSTR(p_inf.infst_tel,1,2) NOT IN (
					'11', '12', '13', '14', '15', '16', '17', '18', '19',
					'21', '22', '24', '27', '28',
					'31', '32', '33', '34', '35', '37', '38',
					'41', '42', '43', '44', '45', '46', '47', '48', '49',
					'51', '53', '54', '55',
					'61', '62', '63', '64', '65', '66', '67', '68', '69',
					'71', '73', '74', '75', '77', '79',
					'81', '82', '83', '84', '85', '86', '87', '88', '89',
					'91', '92', '93', '94', '95', '96', '97', '98', '99') THEN
		   v_num_terminal := '1135497777';
		ELSIF SUBSTR(p_inf.infst_tel,3,1) = '0' THEN
		   WITH num AS (
			  -- SELECT '320988060535' CADG_NUM_CONTA FROM dual
			  SELECT p_inf.infst_tel CADG_NUM_CONTA FROM dual
		   ), terminal AS (
			  SELECT SUBSTR(CADG_NUM_CONTA,1,2) AS DDD,
					 SUBSTR(CADG_NUM_CONTA,4) AS TELEFONE_APOS_0,
					 LENGTH(SUBSTR(CADG_NUM_CONTA,4)) AS TAM_TELEFONE_APOS_0,
					 SUBSTR(CADG_NUM_CONTA,4,1) AS PRIMEIRO_DIG_APOS_0
				FROM num
			   WHERE SUBSTR(CADG_NUM_CONTA,3,1) = '0'
		   )
		   SELECT CASE 
				  WHEN PRIMEIRO_DIG_APOS_0 in ('1', '2', '3','4','5','6','7','8','9') AND TAM_TELEFONE_APOS_0 in (8,9) THEN
					 DDD || TELEFONE_APOS_0
				  ELSE
					 '1135497777'
				  END AS NOVO_CADG_NUM_CONTA
			 INTO v_num_terminal
			 FROM TERMINAL;
		ELSE
		   v_num_terminal := p_inf.infst_tel;
		END IF;
	ELSE
		v_num_terminal := CASE WHEN p_cli.cadg_num_conta LIKE '%00000' OR p_cli.cadg_num_conta IS NULL THEN '1135497777' ELSE p_cli.cadg_num_conta END;
	END IF;	

	
	IF p_nr_qtde_inf <= 1 THEN
	
		IF TRIM(p_cli.cadg_cod_cgccpf) IS NOT NULL AND REGEXP_LIKE(TRIM(p_cli.cadg_cod_cgccpf),'[0-9]') THEN
		-- IF SANEAMENTO_GF_FLA.VALIDA_CPF_CNPJ(p_cli.cadg_cod_cgccpf) THEN
		   v_cpf_cnpj := p_cli.cadg_cod_cgccpf;
		-- ELSIF SANEAMENTO_GF_FLA.VALIDA_CPF_CNPJ(TRIM(p_nf.cnpj_cpf)) THEN
		--   v_cpf_cnpj := TRANSLATE(p_nf.cnpj_cpf,'0./- ','0');
		ELSE
		   v_cpf_cnpj := '11111111111';
		END IF;

		IF v_cpf_cnpj != p_nf.cnpj_cpf THEN
		   v_nr_add_nf             := 1;	
		   p_nf.update_reg         := 1;	
		   p_nf.var05              := SUBSTR('ANT_CPF_CNPJ=' || p_nf.cnpj_cpf  || '>>' ||p_nf.var05,1,150);
		   p_nf.cnpj_cpf           := v_cpf_cnpj;	 
		END IF;
		
		IF v_cpf_cnpj != p_cli.cadg_cod_cgccpf THEN
			v_nr_add_cli           := 1;
			p_cli.update_reg       := 1;
			p_cli.var05_cli        := SUBSTR(' CLI: ' || p_cli.cadg_cod_cgccpf || '|' || p_cli.cadg_tip  || '>>' ||p_cli.var05_cli,1,150);
			p_cli.cadg_cod_cgccpf  := v_cpf_cnpj;
			p_cli.cadg_tip         := CASE LENGTH(p_cli.cadg_cod_cgccpf)
										   WHEN 11 THEN 'F'
										   WHEN 14 THEN 'J'
										   ELSE p_cli.cadg_tip
									  END;
		END IF;		
	
	END IF;

	IF  NVL(p_sanea.cfop_max,'_0_') = '0000'
	THEN
		v_cadg_tip_cli        := '99';
		v_cadg_tip_assin      := '6';
	ELSIF 
		-- NVL(p_inf.infst_base_icms,0) > 0 AND 
		NVL(p_inf.cfop,'_0_') != '0000' 
	THEN
	
		v_cadg_tip_cli := CASE LENGTH(p_nf.cnpj_cpf)
							   WHEN 11 THEN '03'
							   WHEN 14 THEN
								  CASE WHEN p_inf.cfop LIKE '_303' THEN '01' WHEN p_inf.cfop LIKE '_302' THEN '02' WHEN p_inf.cfop LIKE '_306' THEN '04' ELSE '99' END
							   ELSE '99'
						  END;

		v_cadg_tip_assin := CASE LENGTH(p_nf.cnpj_cpf)
								WHEN 11 THEN '3'
								WHEN 14 THEN
								   CASE WHEN p_inf.cfop LIKE '_302' THEN '1' WHEN p_inf.cfop LIKE '_303' THEN '1' WHEN p_inf.cfop = '5306' THEN '4' ELSE '6' END
								ELSE '6'
							END;

	ELSE
		v_cadg_tip_cli      := NVL(p_cli.cadg_tip_cli,'X');
		v_cadg_tip_assin    := NVL(p_cli.cadg_tip_assin,'X');
	END IF;
	
	IF v_cadg_tip_cli != NVL(p_cli.cadg_tip_cli,'X') OR v_cadg_tip_assin != NVL(p_cli.cadg_tip_assin,'X')  THEN
		v_nr_add_comp             := 1;
		p_cli.update_reg_comp     := 1;
		p_cli.var05_cli           := SUBSTR(' COMP: ' || p_cli.cadg_tip_assin || '|' || p_cli.cadg_tip_cli  || '>>' ||p_cli.var05_cli,1,150);
		p_cli.cadg_tip_assin      := v_cadg_tip_assin;
		p_cli.cadg_tip_cli        := v_cadg_tip_cli; 
	END IF;


	IF p_nf.cnpj_cpf != p_inf.cgc_cpf OR p_cli.cadg_cod_insest != p_inf.ie OR v_num_terminal != p_inf.infst_tel THEN
	   v_nr_add_inf             := 1;
	   p_inf.update_reg         := 1;	
	   p_inf.var05              := SUBSTR('ANT_CGC_CPF=' ||'_IE='||p_inf.IE||'_INFST_TEL='||p_inf.INFST_TEL|| '>>' ||p_inf.var05,1,150);
	   p_inf.cgc_cpf            := p_nf.cnpj_cpf;	
	   p_inf.ie                 := p_cli.cadg_cod_insest;	
	   p_inf.infst_tel          := v_num_terminal;	
	END IF;	

	IF p_cli.CADG_DAT_ATUA >= TO_DATE('01/01/2017','DD/MM/YYYY') AND NVL(p_cli.cadg_tip_assin,0)  != 0 THEN
		p_cli.update_reg_comp     := 1;
		p_cli.var05_cli           := SUBSTR('TIP_ASS[0]: ' || p_cli.cadg_tip_assin  || '>>' ||p_cli.var05_cli,1,150);
		p_cli.cadg_tip_assin      := 0;	
	END IF;
							
	IF v_nr_add_nf    > 0
    THEN	
		v_nf_rules               := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|24X7|',
												  p_nr_var01  =>  1);  
	END IF;
	
	IF v_nr_add_inf    > 0 AND NVL(p_inf.update_reg,0) != 0 AND p_inf.rowid_inf IS NOT NULL
    THEN	
		v_inf_rules             := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
												 p_nm_var01      =>  p_inf.emps_cod,
		                                         p_nm_var02      =>  p_inf.fili_cod,
		                                         p_nm_var03      =>  p_inf.infst_serie,
						                         p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
		                                         p_nm_var05      =>  '|24X7|',
						                         p_nr_var02      =>  1);
	END IF;
	
	IF v_nr_add_cli    > 0
    THEN	
		v_cli_rules              := fncts_add_var(p_ds_rules  =>  v_cli_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|24X7|',
												  p_nr_var03  =>  1); 		
	END IF;
	
	IF v_nr_add_comp    > 0
    THEN	
		v_cli_rules_comp         := fncts_add_var(p_ds_rules  =>  v_cli_rules_comp, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|24X7|',
												  p_nr_var04  =>  1); 		

	END IF;
		
END;
--/	