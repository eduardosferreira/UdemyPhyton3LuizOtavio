-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_31(
    p_nf             IN OUT NOCOPY c_nf%rowtype,
    p_inf            IN c_inf%ROWTYPE,
	p_st_t           IN c_st%ROWTYPE,
	p_cli            IN c_cli%ROWTYPE)
AS
BEGIN

    -- Guardar os valores dos tIpos de utlização dos itens.  OBS: Considera o tipo de utilizacao do item nulo com valor igual a 1  
    v_reg_31_sev_cod_tip_uti_inf := SUBSTR('|' || NVL(TRIM(TO_CHAR(p_st_t.servtl_tip_utiliz)),'1') || '|,' || v_reg_31_sev_cod_tip_uti_inf,1,32767);
	IF TRIM(TO_CHAR(v_reg_31_sev_cod_tip_uti_min)) IS NULL THEN 
		v_reg_31_sev_cod_tip_uti_min := NVL(TRIM(TO_CHAR(p_st_t.servtl_tip_utiliz)),'1');
	ELSE
		IF	NVL(TRIM(TO_CHAR(p_st_t.servtl_tip_utiliz)),'1') < TRIM(TO_CHAR(v_reg_31_sev_cod_tip_uti_min)) THEN
			v_reg_31_sev_cod_tip_uti_min := p_st_t.servtl_tip_utiliz;
		END IF;	
	END IF;
	
	-- Valida quando for ultimo registro referente ao ITENS da NOTA FISCAL ! Será sempre executado no ultimo registro do item da NF.
	IF UPPER(TRIM(p_inf.last_reg_nf)) = 'S' THEN
	
		DECLARE

			v_reg_31_exists             NUMBER        := 0; 
			v_reg_31_mnfst_tip_util     openrisow.mestre_nftl_serv.mnfst_tip_util%type;
		
		BEGIN
		
			v_reg_31_mnfst_tip_util  := p_nf.mnfst_tip_util;
			
			-- 1 ) Se tipo de utilizacao da nota fiscal for nulo, faça:
			IF TRIM(TO_CHAR(v_reg_31_mnfst_tip_util)) IS NULL THEN		
				-- 1.1 ) Se tipo de utilizacao do cliente não for nulo, faça:
				IF TRIM(TO_CHAR(p_cli.cadg_tip_utiliz)) IS NOT NULL THEN
					-- 1.1.1) Verifica tipo de utilizacao do cliente, se existe em um dos itens da nota fiscal. OBS: Considera o tipo de utilizacao do cliente nulo com valor igual a 1  
					IF  INSTR(v_reg_31_sev_cod_tip_uti_inf,'|' || NVL(TRIM(TO_CHAR(p_cli.cadg_tip_utiliz)),'1') || '|') > 0 THEN
						-- 1.1.1.1 ) Caso encontre, atribua no valor da nota fiscal.
						v_reg_31_mnfst_tip_util  := TRIM(TO_CHAR(p_cli.cadg_tip_utiliz));
						v_reg_31_exists    	  := 1;
					END IF;	
				END IF;			
			ELSE
				-- 2 ) Se tipo de utilizacao da nota fiscal NÃO for nulo, faça:
				-- 2.1.1) Verifica tipo de utilizacao da nota fiscal, se existe em um dos itens da nota fiscal.
				IF INSTR(v_reg_31_sev_cod_tip_uti_inf, '|' || TRIM(TO_CHAR(v_reg_31_mnfst_tip_util)) || '|') > 0 THEN
					v_reg_31_exists    	     := 1;
				END IF;			
			END IF;

			-- 3 ) Caso não encontre nenhum dos casos citados acima, busca o menor tipo de utilizacao dos itens
			IF v_reg_31_exists  = 0 THEN
				v_reg_31_mnfst_tip_util      := TRIM(TO_CHAR(v_reg_31_sev_cod_tip_uti_min));
			END IF;
			
			IF NVL(TRIM(TO_CHAR(v_reg_31_mnfst_tip_util)),'_0_') != NVL(TRIM(TO_CHAR(p_nf.mnfst_tip_util)),'_0_')
			THEN
			  p_nf.update_reg        := 1;
			  p_nf.var05             := SUBSTR('prcts_regra_31u:' || p_nf.mnfst_tip_util || '>>' ||p_nf.var05,1,150);
			  p_nf.mnfst_tip_util    := TRIM(TO_CHAR(v_reg_31_mnfst_tip_util));	 
			  v_nf_rules             := fncts_add_var(p_ds_rules  =>  v_nf_rules, 
												  p_nm_var01  =>  p_nf.emps_cod,
						                          p_nm_var02  =>  p_nf.fili_cod,
						                          p_nm_var03  =>  p_nf.mnfst_serie,
												  p_nm_var04  =>  TO_CHAR(p_nf.mnfst_dtemiss,'YYYY-MM-DD'),
		                                          p_nm_var05  =>  '|R2015_31|',
												  p_nr_var01  =>  1); 
			END IF;
			
			v_reg_31_sev_cod_tip_uti_inf := NULL;
			v_reg_31_sev_cod_tip_uti_min := NULL;

		END;

	END IF;

END;
--/	