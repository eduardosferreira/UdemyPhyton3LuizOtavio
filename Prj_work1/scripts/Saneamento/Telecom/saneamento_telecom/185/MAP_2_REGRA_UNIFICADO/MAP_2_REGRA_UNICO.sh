#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
FILTRO_SCRIPT="${4:-${FILTRO}}"
REGRAS_SCRIPT="${5:-${REGRAS_HISTORIAS}}"
PROCESSO_SCRIPT="${6:-${PROCESSO}}"
NOME_SCRIPT="${7:-${SCRIPT}}"
SPOOL_FILE_SCRIPT="${8:-${SPOOL_FILE}}"
STRING_CONEXAO_SCRIPT="${9:-${STRING_CONEXAO}}"
DIRETORIO_SCRIPT="${10:-${DIRETORIO_PRINCIPAL}}"
COMMIT_SCRIPT="${11:-${COMMIT}}"
SEQUENCE_CONTROLE_SCRIPT="${12:-${SEQUENCE_CONTROLE}}"

#tratamento dos parametros
FILTRO_SCRIPT="${FILTRO_SCRIPT^^}" 
FILTRO_SCRIPT="${FILTRO_SCRIPT##*( )}" 
FILTRO_SCRIPT="${FILTRO_SCRIPT%%*( )}"

if [ "${TABELA_NF}" == *"_NFEN_"* -o "${TABELA_INF}" ==  *"_NFEM_"* ]; then
	FINDUNICO="MNFST_"
	REPLACEUNICO="MNFEM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"	
	FINDUNICO="INFST_"
	REPLACEUNICO="INFEM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"		
	FINDUNICO="DTEMISS"
	REPLACEUNICO="DTEMIS"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"
elif [ "${TABELA_NF}" == *"_NFSD_"* -o "${TABELA_INF}" ==  *"_NFSD_"* ]; then
	FINDUNICO="MNFST_"
	REPLACEUNICO="MNFSM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"	
	FINDUNICO="INFST_"
	REPLACEUNICO="INFSM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"		
else
	case ${FILTRO_SCRIPT} in
	""|"1=1")
			FILTRO_SCRIPT="UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1') AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))"	
		;;
		*)
			FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1') AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))" 
		;;
	esac
fi
echo ${FILTRO_SCRIPT}
echo ${PARTICAO_NF}
echo ${PARTICAO_INF}

if [[ $PARTICAO_NF == *"PARTICAO"* ]]; then

	FINDUNICO="PARTICAO"
	REPLACEUNICO=""
	PARTITION_NF="${PARTICAO_NF//$FINDUNICO/$REPLACEUNICO}"	
	PARTITION_INF="${PARTICAO_INF//$FINDUNICO/$REPLACEUNICO}"
	
	if [ "${TABELA_NF}" == *"_NFEN_"* -o "${TABELA_INF}" ==  *"_NFEM_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFEM_DTENTR = TO_DATE('${PARTITION_NF}','DDMMYYYY')"	
	elif [ "${TABELA_NF}" == *"_NFSD_"* -o "${TABELA_INF}" ==  *"_NFSD_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFSM_DTEMISS = TO_DATE('${PARTITION_NF}','DDMMYYYY')"		
	else
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFST_DTEMISS = TO_DATE('${PARTITION_NF}','DDMMYYYY')"	
	fi
	
	PARTITION_NF=""		
	PARTITION_INF=""

elif [[ $PARTICAO_INF == *"PARTICAO"* ]]; then

	FINDUNICO="PARTICAO"
	REPLACEUNICO=""
	PARTITION_NF="${PARTICAO_NF//$FINDUNICO/$REPLACEUNICO}"	
	PARTITION_INF="${PARTICAO_INF//$FINDUNICO/$REPLACEUNICO}"

	if [ "${TABELA_NF}" == *"_NFEN_"* -o "${TABELA_INF}" ==  *"_NFEM_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFEM_DTENTR = TO_DATE('${PARTITION_INF}','DDMMYYYY')"	
	elif [ "${TABELA_NF}" == *"_NFSD_"* -o "${TABELA_INF}" ==  *"_NFSD_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFSM_DTEMISS = TO_DATE('${PARTITION_INF}','DDMMYYYY')"		
	else
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFST_DTEMISS = TO_DATE('${PARTITION_INF}','DDMMYYYY')"	
	fi	
	
	PARTITION_NF=""		
	PARTITION_INF=""

else 

	PARTITION_NF=" PARTITION (${PARTICAO_NF})"	
	PARTITION_INF=" PARTITION (${PARTICAO_INF})"

fi
echo ${FILTRO_SCRIPT}
echo ${PARTITION_NF}
echo ${PARTITION_INF}


REGRAS_SCRIPT="${REGRAS_SCRIPT^^}" 
REGRAS_SCRIPT="${REGRAS_SCRIPT##*( )}" 
REGRAS_SCRIPT="${REGRAS_SCRIPT%%*( )}"

if [ "${TABELA_NF}" == *"_NFEN_"* -o "${TABELA_INF}" ==  *"_NFEM_"* ]; then
	REGRAS_SCRIPT="${REGRAS_SCRIPT^^}" 
elif [ "${TABELA_NF}" == *"_NFSD_"* -o "${TABELA_INF}" ==  *"_NFSD_"* ]; then
	REGRAS_SCRIPT="${REGRAS_SCRIPT^^}" 
else	
	case ${REGRAS_SCRIPT} in
		*"NOVO_MAPA"*|*"novo_mapa"*|*"ABERTURA_MES"*|*"abertura_mes"*)
			REGRAS_SCRIPT="R_CORRIGI_FILI_COD, ERRO_6_34_1, ERRO_6_30, R2015_58, ERRO_6_16, ERRO_ITENS_SEQ, ERRO_6_20, ERRO_TSH_GF_034, ERRO_TSH_GF_002, ERRO_6_37, 24X7,24X7_PE,R2015_63,R2015_36,CFOP_0000, CFOP_5_6,R_SANEA_TE,R2015_6 ,ERRO_122,R2015_31, R2016_39,R2015_57,R2015_56" 
		;;
		*)
			REGRAS_SCRIPT="${REGRAS_SCRIPT^^}" 
		;;
	esac
fi

echo ${REGRAS_SCRIPT}

#turn it on
shopt -s extglob

REPLACEVAZIO=""
REGRA=""
PRCTS_STOP=""
PRCTS_TRATAR_INF=""
PRCTS_TRATAR_NF=""
ATRIBUICAO_NF=""
ATRIBUICAO_ST=""
ATRIBUICAO_CLI=""
PRCTS_TRATAR_CLI=""
ARQUIVOS_SCRIPTS=""
FUNCOES_ANALITICOS=""
CARREGAR_DADOS_MEMORIA=""
PRCTS_TRATAR_NFEM=""
PRCTS_TRATAR_NFSD=""
PRCTS_TRATAR_INFEM=""
PRCTS_TRATAR_INFSD=""

IFS=',' # colon (,) is set as delimiter
read -ra ADDR <<< "$REGRAS_SCRIPT" # str is read into an array as tokens separated by IFS

for i in "${ADDR[@]}"; do # access each element of array

    REGRA="$i"
	### Trim leading whitespaces ###
	REGRA="${REGRA##*( )}" 
	### trim trailing whitespaces  ##
	REGRA="${REGRA%%*( )}"
	REGRA="${REGRA^^}" 	
	
	
	if [ "${REGRA}" == "CORRIGE_MDOC_COD" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_corrige_mdoc_cod(p_inf=> p_inf1| p_nf => p_nf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_corrige_mdoc_cod.sql;"`
		
	elif [ "${REGRA}" == "SERVICO_INEXISTENTE" ]; then

		echo "${REGRA}"	
		ATRIBUICAO_ST=`echo -e ${ATRIBUICAO_ST}'\n'" prcts_etapa('${REGRA}');"`  
		ATRIBUICAO_ST=`echo -e ${ATRIBUICAO_ST}'\n'" prcts_regra_SERV_NAO_EXISTENTE(p_inf => v_inf| p_st_t=> v_st_t);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_SERV_NAO_EXISTENTE.sql;"`
	
	elif [ "${REGRA}" == "R2015_63" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_63(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_63.sql;"`
	
	elif  [ "${REGRA}" == "R2015_62" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_62(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_62.sql;"`
	
	elif  [ "${REGRA}" == "R2015_61" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_61(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_61.sql;"`
	
	elif [ "${REGRA}" == "R2015_60" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_60(p_cli   => p_cli1| p_f => p_f1|p_inf=> p_inf1| p_nf => p_nf1| p_sanea => p_sanea1|p_cp=> p_cp1| p_st_t=> p_st_t1|	 p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_60.sql;"`
		
	elif  [ "${REGRA}" == "R2015_59" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_59(p_inf=> p_inf1|p_nf => p_nf1);"`
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_59.sql;"`

	elif [ "${REGRA}" == "R2015_58" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_58(p_inf=> p_inf1|p_nf => p_nf1);"`
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_58.sql;"`

	elif [ "${REGRA}" == "R2015_57" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_57(p_cli   => p_cli1|p_inf=> p_inf1);"`  	
		CARREGAR_DADOS_MEMORIA=`echo -e ${CARREGAR_DADOS_MEMORIA}'\n'" prcts_regra_57;"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_57.sql;"`
		
	elif  [ "${REGRA}" == "R2015_56" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_56(p_cli   => p_cli1|p_inf=> p_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_56.sql;"`
		
	elif [ "${REGRA}" == "R2015_52" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_52(p_cli   => p_cli1|p_inf=> p_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_52.sql;"`
		
	elif  [ "${REGRA}" == "R2015_51" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_51(p_cli   => p_cli1| p_f => p_f1|p_inf=> p_inf1| p_nf => p_nf1| p_sanea => p_sanea1|p_cp=> p_cp1| p_st_t=> p_st_t1|	 p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_51.sql;"`
		
	elif  [ "${REGRA}" == "R2015_50" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"` 	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_50(p_inf => p_inf1|p_st_t => p_st_t1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_50.sql;"`
	
	elif [ "${REGRA}" == "R2015_49" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_49(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_49.sql;"`
	
	elif [ "${REGRA}" == "R2015_48" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_48(p_cli   => p_cli1| p_f => p_f1|p_inf=> p_inf1| p_nf => p_nf1| p_sanea => p_sanea1|p_cp=> p_cp1| p_st_t=> p_st_t1|	 p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_48.sql;"`
		
	elif [ "${REGRA}" == "R2017_47" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"` 	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_47(p_inf => p_inf1|p_st_t => p_st_t1|p_cli => p_cli1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_47.sql;"`
	
	elif [ "${REGRA}" == "R_ATUALIZA_Z04" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_atualizacaoZ04(p_cli=> p_cli1|p_inf=> p_inf1|p_nf => p_nf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_atualizacaoZ04.sql;"` 
		
	
	elif [ "${REGRA}" == "R2016_41" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_41(p_inf=> p_inf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_41.sql;"` 
	
	elif [ "${REGRA}" == "R2015_29" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_29(p_cli=> p_cli1|p_f=> p_f1|p_inf=> p_inf1|p_nf => p_nf1| p_sanea=> p_sanea1| p_cp => p_cp1|p_st_t=> p_st_t1| p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_29.sql;"`
	
	elif  [ "${REGRA}" == "R2016_42B" ]; then
	 
	 	echo "${REGRA}"	
	 	PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
	 	PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_42B(p_cli=> p_cli1|p_f=> p_f1|p_inf=> p_inf1|p_nf => p_nf1| p_sanea=> p_sanea1| p_cp => p_cp1|p_st_t=> p_st_t1| p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	 
	 	ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_42B.sql;"`
	 
	elif  [ "${REGRA}" == "R2015_24" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_24(p_inf=> p_inf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_24.sql;"` 
		
	
	elif [ "${REGRA}" == "R2015_30" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_30(p_cli=> p_cli1|p_f=> p_f1|p_inf=> p_inf1|p_nf => p_nf1| p_sanea=> p_sanea1| p_cp => p_cp1|p_st_t=> p_st_t1| p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_30.sql;"`
	
	elif  [ "${REGRA}" == "R2015_44" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_44( p_inf       => p_inf1|		  p_sanea     => p_sanea1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_44.sql;"`
		
	elif [ "${REGRA}" == "R2017_43" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_43(p_cli=> p_cli1|p_f=> p_f1|p_inf=> p_inf1|p_nf => p_nf1| p_sanea=> p_sanea1| p_cp => p_cp1|p_st_t=> p_st_t1| p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_43.sql;"`
	
	elif [ "${REGRA}" == "R2016_37" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_37(p_inf=> p_inf1|p_nf => p_nf1|pCOMMIT=>'${COMMIT_SCRIPT}');"`
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_37.sql;"` 
		
	
	elif [ "${REGRA}" == "R_SANEA_CLIENTE_TV" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_saneaCLIENTETV(p_cli=> p_cli1|p_inf=> p_inf1|p_nf => p_nf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_saneaCLIENTETV.sql;"` 
		
	
	elif [ "${REGRA}" == "R_CORRIGI_FILI_COD" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_corrigi_fili_cod(p_inf=> p_inf1|p_nf => p_nf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_corrigi_fili_cod.sql;"` 
		
	
	elif [ "${REGRA}" == "ERRO_6_34_1" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_erro_6_34_1(p_inf=> p_inf1|p_sanea => p_sanea1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_34_1.sql;"` 
		
	
	elif  [ "${REGRA}" == "ERRO_6_30" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_erro_6_30(p_inf=> p_inf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_30.sql;"` 
	
	elif [ "${REGRA}" == "ERRO_6_25" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_erro_6_25(p_inf=> p_inf1|p_nf => p_nf1);"`
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_25.sql;"`

	elif  [ "${REGRA}" == "R_SANEA_TE" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_saneaTE(p_cli=> p_cli1|p_inf=> p_inf1|p_nf => p_nf1|p_sanea => p_sanea1);"`		
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_saneaTE.sql;"`		
		
		FUNCOES_ANALITICOS=`echo -e ${FUNCOES_ANALITICOS}'\n'" FIRST_VALUE(inf.CFOP) over(PARTITION BY nf.rowid ORDER BY CASE WHEN inf.CFOP like '73%'  THEN  0  WHEN inf.CFOP like '%301' THEN  1  WHEN inf.CFOP like '%305' THEN  2   WHEN inf.CFOP like '%306' THEN  3  WHEN inf.CFOP like '%307' THEN  4  ELSE 99	END) cfop_73,"`
		
	elif  [ "${REGRA}" == "ERRO_6_16" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_erro_6_16(p_inf=> p_inf1|p_nf => p_nf1|p_sanea => p_sanea1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_16.sql;"`
	
	elif  [ "${REGRA}" == "ERRO_ITENS_SEQ" -o "${REGRA}" ==  "REORDENACAO_INFST_NUM_SEQ" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_erro_itens_seq(p_inf=> p_inf1);"`
		ARQUIVOS_SCRIPTS="${ARQUIVOS_SCRIPTS//@${DIRETORIO_SCRIPT}prcts_regra_erro_itens_seq.sql;/$REPLACEVAZIO}"
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_itens_seq.sql;"`
       
	   elif [ "${REGRA}" == "ERRO_6_24" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_erro_6_24(p_inf=> p_inf1|p_nf => p_nf1|p_cli => p_cli1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_24.sql;"`
		
	elif [ "${REGRA}" == "24X7" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_24X7(p_inf=> p_inf1|p_nf => p_nf1|p_cli => p_cli1|p_sanea => p_sanea1|p_nr_qtde_inf => p_nr_qtde_inf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_24X7.sql;"`
		
	elif [ "${REGRA}" == "24X7_PE" ]; then
		
		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_24X7_PE(p_inf=> p_inf1|p_nf => p_nf1|p_cli => p_cli1|p_sanea => p_sanea1|p_nr_qtde_inf => p_nr_qtde_inf1);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_24X7_PE.sql;"`
		
	elif  [ "${REGRA}" == "R2015_1" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_1(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_1.sql;"`
	
	elif [ "${REGRA}" == "R2015_2" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_2(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_2.sql;"`
	
	elif [ "${REGRA}" == "R2015_4" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_4(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_4.sql;"`
	
	elif [ "${REGRA}" == "R2015_7" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_7(p_inf => p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_7.sql;"`
	
	elif [ "${REGRA}" == "R2015_9" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_9(p_inf=> p_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_9.sql;"`
	
	elif [ "${REGRA}" == "R2015_19" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_19(p_cli=> p_cli1|p_f=> p_f1|p_inf=> p_inf1|p_nf => p_nf1| p_sanea=> p_sanea1| p_cp => p_cp1|p_st_t=> p_st_t1| p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_19.sql;"`
	
	elif [ "${REGRA}" == "R2015_14" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_14(p_cli   => p_cli1| p_f => p_f1|p_inf=> p_inf1| p_nf => p_nf1| p_sanea => p_sanea1|p_cp=> p_cp1| p_st_t=> p_st_t1|	 p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_14.sql;"`
		
	elif [ "${REGRA}" == "R2015_34" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_34(p_inf=> p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_34.sql;"`

	elif [ "${REGRA}" == "R2015_45" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_45(p_inf=> p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_45.sql;"`
	
	elif [ "${REGRA}" == "CFOP_5_6" -o "${REGRA}" ==  "ERRO_TRATAMENTO_CFOP" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_CFOP_5_6(p_f  => p_f1|  p_inf=> p_inf1);"`  	
		
		ARQUIVOS_SCRIPTS="${ARQUIVOS_SCRIPTS//@${DIRETORIO_SCRIPT}prcts_regra_CFOP_5_6.sql;/$REPLACEVAZIO}"
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_CFOP_5_6.sql;"`
	
	elif [ "${REGRA}" == "R2016_39" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"` 	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_39(p_inf => p_inf1|p_st_t => p_st_t1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_39.sql;"`
	
	elif [ "${REGRA}" == "R2015_36" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_36( p_inf       => p_inf1|		  p_sanea     => p_sanea1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_36.sql;"`
	
	elif [ "${REGRA}" == "R2015_32" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_32( p_inf       => p_inf1|		  p_sanea     => p_sanea1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_32.sql;"`
		
	elif [ "${REGRA}" == "R2015_6" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_6(p_f         => p_f1|p_inf       => p_inf1|p_nf        => p_nf1|	p_sanea     => p_sanea1|p_nr_qtde_inf=> p_nr_qtde_inf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_6.sql;"`
		
	elif [ "${REGRA}" == "CFOP_0000" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_CFOP_0000( p_inf              => p_inf1 | p_st_t => p_st_t1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_CFOP_0000.sql;"`
	
	elif [ "${REGRA}" == "R2015_31" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_regra_31(p_nf        => p_nf1| p_inf              => p_inf1 | p_st_t => p_st_t1 | p_cli   => p_cli1);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_31.sql;"`
		
	elif [ "${REGRA}" == "ERRO_122" ]; then

		echo "${REGRA}"	
		PRCTS_TRATAR_NF=`echo -e ${PRCTS_TRATAR_NF}'\n'" prcts_etapa('${REGRA}');"`  	
		PRCTS_TRATAR_NF=`echo -e ${PRCTS_TRATAR_NF}'\n'" prcts_regra_erro_122(p_f => p_f1|p_nf => p_nf1);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_122.sql;"`
	
	elif [ "${REGRA}" == "ERRO_TSH_GF_002" ]; then

		echo "${REGRA}"	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_etapa('${REGRA}');"`  	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_regra_erro_tsh_gf_002(p_nf        => v_nf);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_tsh_gf_002.sql;"`
	
	elif [ "${REGRA}" == "ERRO_TSH_GF_034" ]; then

		echo "${REGRA}"	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_etapa('${REGRA}');"`  	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_regra_erro_tsh_gf_034(p_nf        => v_nf);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_tsh_gf_034.sql;"`
	
	elif [ "${REGRA}" == "ERRO_6_20" ]; then

		echo "${REGRA}"	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_etapa('${REGRA}');"`  	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_regra_erro_6_20(p_nf        => v_nf);"`  	
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_20.sql;"`
	
	elif [ "${REGRA}" == "ERRO_6_33" ]; then

		echo "${REGRA}"	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_etapa('${REGRA}');"`  	
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_regra_erro_6_33(p_nf        => v_nf);"`  	
	
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_33.sql;"`
		
	elif [ "${REGRA}" == "ERRO_6_37" ]; then
		
		echo "${REGRA}"	
		ATRIBUICAO_CLI=`echo -e ${ATRIBUICAO_CLI}'\n'" prcts_etapa('${REGRA}');"`
		ATRIBUICAO_CLI=`echo -e ${ATRIBUICAO_CLI}'\n'" prcts_regra_erro_6_37(p_cli => v_cli);"`
		
		ARQUIVOS_SCRIPTS=`echo -e ${ARQUIVOS_SCRIPTS}'\n'" @${DIRETORIO_SCRIPT}prcts_regra_erro_6_37.sql;"`
		
	else 
		echo "${REGRA} STOP !!!"
		PRCTS_STOP=`echo -e ${PRCTS_STOP}'\n'" prcts_stop('${REGRA} - Regra nao localizado ! ');"`
		ATRIBUICAO_CLI=`echo -e ${ATRIBUICAO_CLI}'\n'" prcts_stop('${REGRA} - Regra nao localizado ! ');"`
		ATRIBUICAO_NF=`echo -e ${ATRIBUICAO_NF}'\n'" prcts_stop('${REGRA} - Regra nao localizado ! ');"`
		PRCTS_TRATAR_NF=`echo -e ${PRCTS_TRATAR_NF}'\n'" prcts_stop('${REGRA} - Regra nao localizado ! ');"`
		PRCTS_TRATAR_INF=`echo -e ${PRCTS_TRATAR_INF}'\n'" prcts_stop('${REGRA} - Regra nao localizado ! ');"`
		PRCTS_TRATAR_CLI=`echo -e ${PRCTS_TRATAR_CLI}'\n'" prcts_stop('${REGRA} - Regra nao localizado ! ');"`
	fi	
	
done


IFS=' ' # reset to default value after usage

# turn it off
shopt -u extglob

FINDUNICO="|"
REPLACEUNICO=","
PRCTS_TRATAR_NFEM="${PRCTS_TRATAR_NFEM//$FINDUNICO/$REPLACEUNICO}"
PRCTS_TRATAR_NFSD="${PRCTS_TRATAR_NFSD//$FINDUNICO/$REPLACEUNICO}"
PRCTS_TRATAR_INFEM="${PRCTS_TRATAR_INFEM//$FINDUNICO/$REPLACEUNICO}"
PRCTS_TRATAR_INFSD="${PRCTS_TRATAR_INFSD//$FINDUNICO/$REPLACEUNICO}"
PRCTS_TRATAR_INF="${PRCTS_TRATAR_INF//$FINDUNICO/$REPLACEUNICO}"
PRCTS_TRATAR_NF="${PRCTS_TRATAR_NF//$FINDUNICO/$REPLACEUNICO}"
PRCTS_TRATAR_CLI="${PRCTS_TRATAR_CLI//$FINDUNICO/$REPLACEUNICO}"
ATRIBUICAO_NF="${ATRIBUICAO_NF//$FINDUNICO/$REPLACEUNICO}"
ATRIBUICAO_CLI="${ATRIBUICAO_CLI//$FINDUNICO/$REPLACEUNICO}"
ATRIBUICAO_ST="${ATRIBUICAO_ST//$FINDUNICO/$REPLACEUNICO}"
PRCTS_STOP="${PRCTS_STOP//$FINDUNICO/$REPLACEUNICO}"
ARQUIVOS_SCRIPTS="${ARQUIVOS_SCRIPTS//$FINDUNICO/$REPLACEUNICO}"
FUNCOES_ANALITICOS="${FUNCOES_ANALITICOS//$FINDUNICO/$REPLACEUNICO}"
CARREGAR_DADOS_MEMORIA="${CARREGAR_DADOS_MEMORIA//$FINDUNICO/$REPLACEUNICO}"

echo "${PRCTS_TRATAR_NFEM}"	
echo "${PRCTS_TRATAR_NFSD}"	
echo "${PRCTS_TRATAR_INFEM}"	
echo "${PRCTS_TRATAR_INFSD}"	
echo "${CARREGAR_DADOS_MEMORIA}"
echo "${PRCTS_TRATAR_INF}"	
echo "${PRCTS_TRATAR_NF}"	
echo "${PRCTS_TRATAR_CLI}"	
echo "${ATRIBUICAO_ST}"	
echo "${ATRIBUICAO_NF}"	
echo "${ATRIBUICAO_CLI}"	
echo "${PRCTS_STOP}"
echo "${ARQUIVOS_SCRIPTS}"
echo "${FUNCOES_ANALITICOS}"



sqlplus -S /nolog <<@EOF >> ${NOME_SCRIPT}_${PARTICAO_NF}_${PROCESSO_SCRIPT}.log 2>> ${NOME_SCRIPT}_${PARTICAO_NF}_${PROCESSO_SCRIPT}.err
CONNECT ${STRING_CONEXAO_SCRIPT}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE_SCRIPT} 
var v_st_processamento    VARCHAR2(50) = 'Finalizado'
var v_msg_erro            VARCHAR2(4000) = 'SCRIPT_UNIFICADO'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0

var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_ins_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_atu_st          NUMBER         = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT SCRIPT_UNIFICADO
PROMPT ### Inicio do processo ###
PROMPT

<<PRINCIPAL>>
DECLARE

	v_action_name VARCHAR2(32) := substr('SCRIPT_UNIFICADO',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO_SCRIPT}',1,32);
	
	CONSTANTE_LIMIT PLS_INTEGER := 5000; 
	
	l_error_count  NUMBER;    
	ex_dml_errors  EXCEPTION;
	PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
	v_error_bk     VARCHAR2(4000);
		
	CONSTANTE_AMOUNT  CONSTANT       VARCHAR2(100) := '|AMOUNT|'; 
	v_id_kyros_control_process       VARCHAR2(500);	
	TYPE v_tb_kyros_control_process  IS TABLE OF gfcadastro.tctb_kyros_control_process%ROWTYPE INDEX BY v_id_kyros_control_process%TYPE;	
	v_bk_kyros_control_process       v_tb_kyros_control_process;
	v_idx_kyros_control_process v_id_kyros_control_process%TYPE := NULL;
	v_kyros_control_process          gfcadastro.tctb_kyros_control_process%ROWTYPE;

	v_nf_rules        VARCHAR2(32767)     := NULL;
	v_inf_rules       VARCHAR2(32767)     := NULL;
	v_cli_rules       VARCHAR2(32767)     := NULL;
	v_st_t_rules      VARCHAR2(32767)     := NULL;
	v_cli_rules_comp  VARCHAR2(32767)     := NULL;

	-- Variaveis locais
	v_nr_qtde_inf         PLS_INTEGER;    
	v_nr_qtde             NUMBER := 0;   	
	v_nr_max_print        NUMBER := 1;
	v_ds_etapa            VARCHAR2(4000);
	
	-- Funcoes Gerais
	FUNCTION fccts_retira_caracter(p_ds_ddo IN VARCHAR2, p_ds_serie IN VARCHAR2) RETURN VARCHAR2 IS
		v_ds_serie_inf    VARCHAR2(500);
		v_ds_serv_cod_inf VARCHAR2(1000); 
		v_nr_length       PLS_INTEGER;
		v_nr_length_aux   PLS_INTEGER;
	BEGIN		
		v_ds_serie_inf    := TRIM(UPPER(TRANSLATE(p_ds_serie,'| ','|')));
		v_nr_length       := NVL(LENGTH(v_ds_serie_inf),0);
		v_ds_serv_cod_inf := TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p_ds_ddo,v_ds_serie_inf||'02Z',''),v_ds_serie_inf||'ZT',''),v_ds_serie_inf||'ZP',''),v_ds_serie_inf||'ZN',''),v_ds_serie_inf||'C08',''),v_ds_serie_inf||'C09',''),v_ds_serie_inf||'Z',''),v_ds_serie_inf||'C','')); 
		FOR I IN 65..75 LOOP
			v_ds_serv_cod_inf := TRIM(REPLACE(v_ds_serv_cod_inf,v_ds_serie_inf||CHR(I),'')); 
		END LOOP;
		v_nr_length_aux       := NVL(LENGTH(v_ds_serv_cod_inf),0);
		IF v_nr_length_aux > v_nr_length THEN
		IF SUBSTR(v_ds_serv_cod_inf,1,v_nr_length) = v_ds_serie_inf THEN
			v_ds_serv_cod_inf := TRIM(SUBSTR(v_ds_serv_cod_inf,v_nr_length+1,v_nr_length_aux));
		END IF;
		END IF;
		RETURN v_ds_serv_cod_inf;
	END;	
	
	PROCEDURE prcts_stop(p_ds_ddo IN VARCHAR2 := NULL) AS 		
	BEGIN
		BEGIN
			v_ds_etapa := substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ['||v_nr_qtde||']: ' ||  p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		EXCEPTION
		WHEN OTHERS THEN
		NULL;
		END;
		RAISE_APPLICATION_ERROR (-20343, 'STOP! ' || SUBSTR(v_ds_etapa,1,1000));
	END;	
	
	PROCEDURE prcts_debug AS 
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		BEGIN
			UPDATE ${TABELA_CONTROLE} cp   SET cp.ds_msg_erro          = substr(v_ds_etapa ,1,4000)	 WHERE cp.rowid = '${ROWID_CP}';	
			DBMS_APPLICATION_INFO.set_client_info(substr(v_ds_etapa ,1,62));				 	
		EXCEPTION
		WHEN OTHERS THEN
		NULL;
		END;
		COMMIT;
	END;
	
	PROCEDURE prcts_etapa(p_ds_ddo IN VARCHAR2, p_fl_debug IN BOOLEAN := FALSE) AS 
		
	BEGIN
		BEGIN			
			IF p_fl_debug THEN				
				v_ds_etapa := substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ['||v_nr_qtde||']: ' ||  p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
				IF v_nr_qtde <= v_nr_max_print OR p_ds_ddo = 'FIM' THEN
					DBMS_OUTPUT.PUT_LINE(substr(v_ds_etapa ,1,2000));
				END IF;
			ELSE
				v_ds_etapa := substr(' ['||v_nr_qtde||']: ' ||  p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
			END IF;
		EXCEPTION
		WHEN OTHERS THEN
		NULL;
		END;
		IF p_fl_debug THEN
			prcts_debug;
		END IF;
	END;
		
	PROCEDURE prcts_process_log(p_nm_var01         IN gfcadastro.tctb_kyros_control_process.nm_var01%type := NULL,
								p_nm_var02         IN gfcadastro.tctb_kyros_control_process.nm_var02%type := NULL,
								p_nm_var03         IN gfcadastro.tctb_kyros_control_process.nm_var03%type := NULL,
								p_nm_var04         IN gfcadastro.tctb_kyros_control_process.nm_var04%type := NULL,
								p_nm_var05         IN gfcadastro.tctb_kyros_control_process.nm_var05%type := NULL,
								p_ds_var01         IN gfcadastro.tctb_kyros_control_process.ds_var01%type := NULL,
								p_ds_var02         IN gfcadastro.tctb_kyros_control_process.ds_var02%type := NULL,
								p_ds_var03         IN gfcadastro.tctb_kyros_control_process.ds_var03%type := NULL,
								p_ds_var04         IN gfcadastro.tctb_kyros_control_process.ds_var04%type := NULL,
								p_ds_var05         IN gfcadastro.tctb_kyros_control_process.ds_var05%type := NULL,
								p_nr_var01         IN gfcadastro.tctb_kyros_control_process.nr_var01%type := 0,
								p_nr_var02         IN gfcadastro.tctb_kyros_control_process.nr_var02%type := 0,
								p_nr_var03         IN gfcadastro.tctb_kyros_control_process.nr_var03%type := 0,
								p_nr_var04         IN gfcadastro.tctb_kyros_control_process.nr_var04%type := 0,
								p_nr_var05         IN gfcadastro.tctb_kyros_control_process.nr_var05%type := 0
								) AS 
	
		v_cc_control          gfcadastro.tctb_kyros_control_process.cc_control%type;
		v_var_process_log     gfcadastro.tctb_kyros_control_process%ROWTYPE;
	
	BEGIN
		
		
		v_cc_control := 'PK';
		IF TRIM(p_nm_var01) IS NOT NULL THEN
			v_cc_control := SUBSTR(v_cc_control || '|' || UPPER(TRIM(p_nm_var01)),1,1000);
		END IF;
		IF TRIM(p_nm_var02) IS NOT NULL THEN
			v_cc_control := SUBSTR(v_cc_control || '|' || UPPER(TRIM(p_nm_var02)),1,1000);
		END IF;
		IF TRIM(p_nm_var03) IS NOT NULL THEN
			v_cc_control := SUBSTR(v_cc_control || '|' || UPPER(TRIM(p_nm_var03)),1,1000);
		END IF;
		IF TRIM(p_nm_var04) IS NOT NULL THEN
			v_cc_control := SUBSTR(v_cc_control || '|' || UPPER(TRIM(p_nm_var04)),1,1000);
		END IF;
		IF TRIM(p_nm_var05) IS NOT NULL THEN
			v_cc_control := SUBSTR(v_cc_control || '|' || UPPER(TRIM(p_nm_var05)),1,1000);
		END IF;
	
		
		IF  v_bk_kyros_control_process.exists(v_cc_control) THEN
	
			v_var_process_log                                  := v_bk_kyros_control_process(v_cc_control);
			v_var_process_log.dt_var02                         := SYSDATE;		
			
			v_var_process_log.nr_var01                         := nvl(v_var_process_log.nr_var01,0) + NVL(p_nr_var01,0);
			v_var_process_log.nr_var02                         := nvl(v_var_process_log.nr_var02,0) + NVL(p_nr_var02,0);
			v_var_process_log.nr_var03                         := nvl(v_var_process_log.nr_var03,0) + NVL(p_nr_var03,0);
			v_var_process_log.nr_var04                         := nvl(v_var_process_log.nr_var04,0) + NVL(p_nr_var04,0);
			v_var_process_log.nr_var05                         := nvl(v_var_process_log.nr_var05,0) + NVL(p_nr_var05,0);     
	
			
		ELSE
	
			v_var_process_log.id_process                       := ${SEQUENCE_CONTROLE_SCRIPT};	
			v_var_process_log.cc_control			           := v_cc_control;			
			v_var_process_log.dt_created                       := SYSDATE;
			v_var_process_log.dt_var01                         := SYSDATE;
			v_var_process_log.dt_var02                         := SYSDATE;		
			
			v_var_process_log.nm_var01                         := UPPER(TRIM(p_nm_var01));			
			v_var_process_log.nm_var02                         := UPPER(TRIM(p_nm_var02));			
			v_var_process_log.nm_var03                         := UPPER(TRIM(p_nm_var03));			
			v_var_process_log.nm_var04                         := UPPER(TRIM(p_nm_var04));			
			v_var_process_log.nm_var05                         := UPPER(TRIM(p_nm_var05));			
						
			v_var_process_log.ds_var01                         := UPPER(TRIM(p_ds_var01));			
			v_var_process_log.ds_var02                         := UPPER(TRIM(p_ds_var02));			
			v_var_process_log.ds_var03                         := UPPER(TRIM(p_ds_var03));			
			v_var_process_log.ds_var04                         := UPPER(TRIM(p_ds_var04));			
			v_var_process_log.ds_var05                         := UPPER(TRIM(p_ds_var05));			
			
			v_var_process_log.nr_var01                         := p_nr_var01;
			v_var_process_log.nr_var02                         := p_nr_var02;     
			v_var_process_log.nr_var03                         := p_nr_var03;     
			v_var_process_log.nr_var04                         := p_nr_var04;     
			v_var_process_log.nr_var05                         := p_nr_var05;     
			
		END IF;
	
		v_bk_kyros_control_process(v_cc_control) 	               := v_var_process_log;
	
	END;
	
	
	FUNCTION fncts_add_var(p_ds_rules         IN VARCHAR2,
						p_nm_var01         IN gfcadastro.tctb_kyros_control_process.nm_var01%type := NULL,
						p_nm_var02         IN gfcadastro.tctb_kyros_control_process.nm_var02%type := NULL,
						p_nm_var03         IN gfcadastro.tctb_kyros_control_process.nm_var03%type := NULL,
						p_nm_var04         IN gfcadastro.tctb_kyros_control_process.nm_var04%type := NULL,
						p_nm_var05         IN gfcadastro.tctb_kyros_control_process.nm_var05%type := NULL,
						p_ds_var01         IN gfcadastro.tctb_kyros_control_process.ds_var01%type := NULL,
						p_ds_var02         IN gfcadastro.tctb_kyros_control_process.ds_var02%type := NULL,
						p_ds_var03         IN gfcadastro.tctb_kyros_control_process.ds_var03%type := NULL,
						p_ds_var04         IN gfcadastro.tctb_kyros_control_process.ds_var04%type := NULL,
						p_ds_var05         IN gfcadastro.tctb_kyros_control_process.ds_var05%type := NULL,
						p_nr_var01         IN gfcadastro.tctb_kyros_control_process.nr_var01%type := 0,
						p_nr_var02         IN gfcadastro.tctb_kyros_control_process.nr_var02%type := 0,
						p_nr_var03         IN gfcadastro.tctb_kyros_control_process.nr_var03%type := 0,
						p_nr_var04         IN gfcadastro.tctb_kyros_control_process.nr_var04%type := 0,
						p_nr_var05         IN gfcadastro.tctb_kyros_control_process.nr_var05%type := 0
						)  
						RETURN VARCHAR2
	AS
		v_ds_value    gfcadastro.tctb_kyros_control_process.ds_var05%type := UPPER(TRIM(p_nm_var05));
		v_ds_rules    VARCHAR2(32767)                                     := UPPER(TRIM(p_ds_rules));
		v_nr_add     PLS_INTEGER := 0;  
	BEGIN
		IF v_ds_value IS NOT NULL THEN
			IF v_ds_rules  IS NOT NULL THEN
				IF  INSTR(v_ds_rules, v_ds_value) = 0 
				THEN
					v_ds_rules := substr(v_ds_value || ',' || v_ds_rules,1,32767);
					v_nr_add   := 1;
				END IF;
			ELSE
				v_ds_rules := v_ds_value;
				v_nr_add   := 1;
			END IF;
		END IF;
		IF v_nr_add  = 1
		AND   (p_nr_var01 > 0   
			or p_nr_var02 > 0   
			or p_nr_var03 > 0   
			or p_nr_var04 > 0   
			or p_nr_var05 > 0) 
		THEN
			prcts_process_log(p_nm_var01  => p_nm_var01,
								p_nm_var02  => p_nm_var02,
								p_nm_var03  => p_nm_var03,
								p_nm_var04  => p_nm_var04,
								p_nm_var05  => v_ds_value,
								p_ds_var01  => p_ds_var01,
								p_ds_var02  => p_ds_var02,
								p_ds_var03  => p_ds_var03,
								p_ds_var04  => p_ds_var04,
								p_ds_var05  => p_ds_var05,
								p_nr_var01  => p_nr_var01,
								p_nr_var02  => p_nr_var02,
								p_nr_var03  => p_nr_var03,
								p_nr_var04  => p_nr_var04,
								p_nr_var05  => p_nr_var05);
				
		END IF;
		RETURN v_ds_rules;
	END;
	
	PROCEDURE prcts_insere_process_log AS 
		PRAGMA AUTONOMOUS_TRANSACTION;
		
		v_idx_log v_id_kyros_control_process%TYPE;
	
	BEGIN
		
		prcts_etapa('prcts_insere_process_log: ' || nvl(v_bk_kyros_control_process.COUNT,0));
		
		v_idx_log        := v_bk_kyros_control_process.first;
		
		WHILE (v_idx_log IS NOT NULL)
		LOOP
		
			-- IF v_bk_kyros_control_process.exists(v_idx_log) THEN
			
				BEGIN
					INSERT INTO gfcadastro.tctb_kyros_control_process 
					VALUES v_bk_kyros_control_process(v_idx_log);
				EXCEPTION
				WHEN DUP_VAL_ON_INDEX THEN
					UPDATE gfcadastro.tctb_kyros_control_process a
					SET a.dt_var02 = v_bk_kyros_control_process(v_idx_log).dt_var02
					,   a.nr_var01 = nvl(a.nr_var01,0) + NVL(v_bk_kyros_control_process(v_idx_log).nr_var01,0)   
					,   a.nr_var02 = nvl(a.nr_var02,0) + NVL(v_bk_kyros_control_process(v_idx_log).nr_var02,0)   
					,   a.nr_var03 = nvl(a.nr_var03,0) + NVL(v_bk_kyros_control_process(v_idx_log).nr_var03,0)
					,   a.nr_var04 = nvl(a.nr_var04,0) + NVL(v_bk_kyros_control_process(v_idx_log).nr_var04,0)   
					,   a.nr_var05 = nvl(a.nr_var05,0) + NVL(v_bk_kyros_control_process(v_idx_log).nr_var05,0)
					WHERE a.id_process = v_bk_kyros_control_process(v_idx_log).id_process
					AND   a.cc_control = v_bk_kyros_control_process(v_idx_log).cc_control;
				END;
				COMMIT;
			
			-- END IF;
			
			v_idx_log := v_bk_kyros_control_process.next(v_idx_log);
		
		END LOOP;
		
		v_bk_kyros_control_process.delete;
		
		COMMIT;
		
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;
			prcts_stop('erro prcts_insere_process_log: ' || SQLERRM);
	END;
		
BEGIN


	IF TRIM(UPPER('${TABELA_NF}')) LIKE '%NFEN%' OR TRIM(UPPER('${TABELA_INF}')) LIKE '%NFEM%' THEN
		GOTO ENTRADA;
	ELSIF TRIM(UPPER('${TABELA_NF}')) LIKE '%NFSD%' OR TRIM(UPPER('${TABELA_INF}')) LIKE '%NFSD%' THEN
		GOTO SAIDA;
	ELSE
		GOTO TELCOM;
	END IF;	
	RETURN;
			
	<<ENTRADA>>
	BEGIN
		NULL;
		
		GOTO FIM;
		
		RETURN;		
	END;
		
	<<SAIDA>>
	BEGIN
		NULL;
		
		GOTO FIM;
		
		RETURN;		
	END;
		
	<<TELCOM>>
	DECLARE
	
		v_reg_31_sev_cod_tip_uti_min  openrisow.servico_telcom.servtl_tip_utiliz%type;
		v_reg_31_sev_cod_tip_uti_inf  VARCHAR2(32767) := NULL;
	
		-- Cursores 
		CURSOR c_sanea
		IS
			WITH tab1 AS
			(SELECT	
				/*+ index(nf MESTRE_NFTL_SERVP1) CURSOR_SHARING_FORCE */ 		  
				ROW_NUMBER() OVER (PARTITION BY nf.rowid ORDER BY 'X') nu_nf,
				-- Funcoes Analiticas
				COUNT(DISTINCT inf.cfop) OVER (PARTITION BY nf.rowid, inf.serv_cod) infst_qtde_cfop_serv_nf,
				COUNT(DISTINCT inf.cfop) OVER (PARTITION BY inf.emps_cod, inf.fili_cod, inf.infst_serie, inf.infst_num, inf.infst_dtemiss, inf.serv_cod) infst_qtde_cfop_serv_inf,
				MAX(inf.cfop) OVER (PARTITION BY inf.emps_cod, inf.fili_cod, inf.infst_serie, inf.infst_num, inf.infst_dtemiss, inf.serv_cod) infst_max_cfop_serv_inf,
				CASE  
					WHEN nf.mnfst_ind_canc IN ('S', 'R', 'C', 'N') THEN nf.mnfst_ind_canc
					ELSE first_value(inf.infst_ind_canc) over (partition BY nf.rowid order by DECODE(inf.infst_ind_canc,'S', 1, 'R', 1, 'C', 1, 'N', 1, 2))
				END mnfst_ind_canc_certo ,
				NVL(TRIM(TO_CHAR(COUNT(1) OVER (PARTITION BY nf.rowid),'00000')),'00000') AS mnfst_ind_cont_aux,
				NULLIF(ROW_NUMBER() over(PARTITION BY nf.rowid ORDER BY
				CASE inf.infst_num_seq
				WHEN 0
				THEN NULL
				ELSE inf.infst_num_seq
				END NULLs LAST),inf.infst_num_seq) infst_num_seq_aux ,
				MAX(inf.infst_num_seq) OVER ( PARTITION BY nf.rowid ) infst_num_seq_max,	
				MAX(inf.cfop) OVER ( PARTITION BY nf.rowid ) cfop_max,		
				${FUNCOES_ANALITICOS}
				-- MIN(inf.cfop) OVER ( PARTITION BY nf.rowid ) cfop_min,		
				-- Concatenacao
				nf.catg_cod	|| '|' || nf.cadg_cod AS cli,
				upper(trim(TRANSLATE(nf.mnfst_serie,'x ','x'))) serie,		
				-- NF
				nf.rowid AS rowid_nf,
				-- nf.mnfst_num,
				nf.catg_cod AS mnfst_catg_cod,
				nf.cadg_cod AS mnfst_cadg_cod,
				-- nf.emps_cod AS mnfst_emps_cod,
				-- nf.fili_cod AS mnfst_fili_cod,
				-- nf.tdoc_cod AS mnfst_tdoc_cod,
				-- nf.mnfst_serie ,
				-- nf.mnfst_dtemiss ,
				nf.mnfst_ind_cont ,			
				nf.mdoc_cod                                                  AS mnfst_mdoc_cod,
				nf.mnfst_val_tot ,
				nf.mnfst_val_desc ,
				nf.mnfst_ind_canc ,
				-- nf.mnfst_dat_venc ,
				nf.mnfst_per_ref ,
				-- nf.mnfst_avista ,
				-- nf.num01 AS mnfst_num01,
				-- nf.num02 AS mnfst_num02,
				-- nf.num03 AS mnfst_num03,
				-- nf.var01 AS mnfst_var01,
				-- nf.var02 AS mnfst_var02,
				-- nf.var03 AS mnfst_var03,
				-- nf.var04 AS mnfst_var04,
				nf.var05 AS mnfst_var05,
				-- nf.mnfst_ind_cnv115 ,
				nf.cnpj_cpf AS mnfst_cnpj_cpf,
				nf.mnfst_val_basicms ,
				nf.mnfst_val_icms ,
				nf.mnfst_val_isentas ,
				nf.mnfst_val_outras ,
				nf.mnfst_codh_nf ,
				-- nf.mnfst_codh_regnf ,
				-- nf.mnfst_codh_regcli ,
				-- nf.mnfst_reg_esp ,
				-- nf.mnfst_bas_icms_st ,
				-- nf.mnfst_val_icms_st ,
				-- nf.mnfst_val_pis ,
				--- nf.mnfst_val_cofins ,
				-- nf.mnfst_val_da ,
				nf.mnfst_val_ser ,
				-- nf.mnfst_val_terc ,
				-- nf.cicd_cod_inf AS mnfst_cicd_cod_inf,
				-- nf.mnfst_tip_assi ,
				nf.mnfst_tip_util ,
				-- nf.mnfst_grp_tens ,
				-- nf.mnfst_ind_extemp ,
				-- nf.mnfst_dat_extemp ,
				-- nf.mnfst_num_fic ,
				-- nf.mnfst_dt_lt_ant ,
				-- nf.mnfst_dt_lt_atu ,
				nf.mnfst_num_fat ,
				-- nf.mnfst_vl_tot_fat ,
				-- nf.mnfst_chv_nfe ,
				-- nf.mnfst_dat_aut_nfe ,
				-- nf.mnfst_val_desc_pis ,
				-- nf.mnfst_val_desc_cofins ,
				-- INF
				inf.rowid AS rowid_inf,
				inf.emps_cod ,
				inf.fili_cod ,
				inf.cgc_cpf ,
				inf.ie ,
				inf.uf ,
				-- inf.tp_loc ,
				-- inf.localidade ,
				-- inf.tdoc_cod ,
				inf.infst_serie ,
				inf.infst_num ,
				inf.infst_dtemiss ,
				inf.catg_cod AS infst_catg_cod,
				inf.cadg_cod AS infst_cadg_cod ,
				inf.serv_cod ,
				inf.estb_cod ,
				inf.infst_dsc_compl ,
				inf.infst_val_cont ,
				inf.infst_val_serv ,
				inf.infst_val_desc ,
				inf.infst_aliq_icms ,
				inf.infst_base_icms ,
				inf.infst_val_icms ,
				inf.infst_isenta_icms ,
				inf.infst_outras_icms ,
				-- inf.infst_tribipi ,
				inf.infst_tribicms ,
				-- inf.infst_isenta_ipi ,
				-- inf.infst_outra_ipi ,
				-- inf.infst_outras_desp ,
				-- inf.infst_fiscal ,
				inf.infst_num_seq ,
				inf.infst_tel ,
				inf.infst_ind_canc ,
				-- inf.infst_proter ,
				-- inf.infst_cod_cont ,
				inf.cfop ,
				inf.mdoc_cod ,
				-- inf.cod_prest ,
				-- inf.num01 ,
				-- inf.num02 ,
				-- inf.num03 ,
				-- inf.var01 ,
				-- inf.var02 ,
				-- inf.var03 ,
				-- inf.var04 ,			
				-- inf.infst_ind_cnv115 ,
				-- inf.infst_unid_medida ,
				-- inf.infst_quant_contr ,
				-- inf.infst_quant_prest ,
				-- inf.infst_codh_reg ,
				inf.esta_cod ,
				inf.infst_val_pis ,
				inf.infst_val_cofins ,
				-- inf.infst_bas_icms_st ,
				-- inf.infst_aliq_icms_st ,
				-- inf.infst_val_icms_st ,
				inf.infst_val_red ,
				-- inf.tpis_cod ,
				-- inf.tcof_cod ,
				-- inf.infst_bas_piscof ,
				-- inf.infst_aliq_pis ,
				-- inf.infst_aliq_cofins ,
				-- inf.infst_nat_rec ,
				-- inf.cscp_cod ,
				inf.infst_num_contr ,
				-- inf.infst_tip_isencao ,
				-- inf.infst_tar_aplic ,
				-- inf.infst_ind_desc ,
				-- inf.infst_num_fat ,
				-- inf.infst_qtd_fat ,
				-- inf.infst_mod_ativ ,
				-- inf.infst_hora_ativ ,
				-- inf.infst_id_equip ,
				-- inf.infst_mod_pgto ,
				-- inf.infst_num_nfe ,
				-- inf.infst_dtemiss_nfe ,
				-- inf.infst_val_cred_nfe ,
				-- inf.infst_cnpj_can_com ,
				-- inf.infst_val_desc_pis ,
				-- inf.infst_val_desc_cofins,
				inf.var05 
			FROM openrisow.item_nftl_serv ${PARTITION_INF}inf,
				openrisow.mestre_nftl_serv ${PARTITION_NF} nf
			WHERE ${FILTRO_SCRIPT} 
			AND inf.emps_cod      = nf.emps_cod
			AND inf.fili_cod      = nf.fili_cod
			AND inf.infst_serie   = nf.mnfst_serie
			AND inf.infst_num     = nf.mnfst_num
			AND inf.infst_dtemiss = nf.mnfst_dtemiss
		),
			tab2 AS
			(SELECT tmp.*,
				comp.rowid rowid_comp,
				comp.cadg_num_conta,
				comp.cadg_tip_assin,
				comp.cadg_tip_cli,
				comp.cadg_dat_atua cadg_dat_atua_comp,
				comp.cadg_cod      cadg_cod_comp,
				comp.catg_cod      catg_cod_comp,
				comp.cadg_uf_habilit,
				comp.cadg_grp_tensao,
				comp.cadg_tip_utiliz
				, comp.cadg_tel_contato
				, comp.var05  AS var05_comp
			FROM openrisow.complvu_clifornec comp,
				(SELECT nf.rowid_nf  AS rowid_nf_cli,
						cli.rowid    AS rowid_cli,
						cli.cadg_cod AS cadg_cod_cli,
						cli.catg_cod AS catg_cod_cli,
						cli.cadg_dat_atua,
						cli.cadg_cod_cgccpf,
						cli.cadg_cod_insest,
						-- cli.cadg_tel,
						-- cli.cadg_ddd_tel,
						-- cli.pais_cod       AS pais_cod_cli,
						-- cli.loca_cod       AS loca_cod_cli,
						cli.unfe_sig       AS unfe_sig_cli,
						cli.mibge_cod_mun  AS mibge_cod_mun_cli,
						cli.cadg_end_munic AS cadg_end_munic_cli,
						cli.cadg_end_cep,
						cli.cadg_end_bairro,
						-- cli.cadg_end_comp,
						-- cli.cadg_end_num,
						-- cli.cadg_end,
						-- cli.cadg_nom_fantasia,
						-- cli.cadg_nom,
						cli.cadg_tip,
						-- cli.tp_loc AS tp_loc_cli,
						cli.var05  AS var05_cli,
						ROW_NUMBER() OVER (PARTITION BY nf.rowid_nf ORDER BY cli.cadg_dat_atua DESC) nu
					FROM openrisow.cli_fornec_transp cli,
						tab1 nf
					WHERE nf.nu_nf = 1
						AND cli.cadg_cod       = nf.mnfst_cadg_cod
						AND cli.catg_cod       = nf.mnfst_catg_cod
						AND cli.cadg_dat_atua <= nf.infst_dtemiss
				) tmp
			WHERE nu               = 1
			AND comp.cadg_cod      = tmp.cadg_cod_cli
			AND comp.catg_cod      = tmp.catg_cod_cli
			AND comp.cadg_dat_atua = tmp.cadg_dat_atua
		)
			, tab3 AS (	SELECT  tmp.* 
						FROM (SELECT tab1.rowid_nf AS rowid_nf_st, 
									tab1.rowid_inf AS rowid_inf_st,
									tab1.mnfst_tip_util AS mnfst_tip_util_st,
									st.rowid AS rowid_st, st.servtl_dat_atua, st.servtl_dat_atua AS servtl_dat_atual,st.servtl_cod,st.clasfi_cod,st.servtl_desc,	st.servtl_compl,st.servtl_ind_tprec,st.servtl_ind_tpserv,st.servtl_cod_nat,	st.var01 AS servtl_var01,st.var02 AS servtl_var02, st.var03 AS servtl_var03,st.var04 AS servtl_var04,st.var05 AS servtl_var05,st.num01 AS servtl_num01,st.num02 AS servtl_num02,st.num03 AS servtl_num03,st.servtl_ind_rec,
									st.servtl_tip_utiliz,	
									ROW_NUMBER() OVER (PARTITION BY tab1.rowid_inf ORDER BY st.servtl_dat_atua DESC) nu_t3	
							FROM   openrisow.servico_telcom st,	
									tab1		
							WHERE  st.emps_cod          = tab1.emps_cod  
							AND    st.fili_cod          = tab1.fili_cod	  
							AND    st.servtl_cod        = tab1.serv_cod	  
							AND    st.servtl_dat_atua  <= tab1.infst_dtemiss ) tmp	
						WHERE 	 tmp.nu_t3 = 1 )
		SELECT /*+ PARALLEL (15) */
		CASE LEAD(t2.rowid_cli , 1) OVER (ORDER BY t2.rowid_cli )
			WHEN t2.rowid_cli
			THEN 'N'
			ELSE 'S'
		END AS last_reg_cli,
		CASE LEAD(t1.rowid_nf, 1) OVER (ORDER BY t2.rowid_cli, t1.rowid_nf, t1.infst_num_seq_aux)
			WHEN t1.rowid_nf
			THEN 'N'
			ELSE 'S'
		END AS last_reg_nf,
		t1.* ,
		t2.* , 
		t3.* 
		FROM tab2 t2
		, tab1 t1 
		, tab3 t3  
		WHERE t2.rowid_nf_cli     = t1.rowid_nf 
		AND   t3.rowid_nf_st  (+) = t1.rowid_nf	
		AND   t3.rowid_inf_st (+) = t1.rowid_inf
		ORDER BY rowid_cli ,   
				last_reg_cli ,  
				rowid_nf ,  
				last_reg_nf , 
				infst_num_seq_aux;
		-- 247
		TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
		v_bk_sanea t_sanea;
		v_sanea    c_sanea%ROWTYPE;
		-- 251
		CURSOR c_cp( p_nm_processo gfcadastro.controle_processamento.nm_processo%type := NULL, p_dt_limite_inf_nf_ini gfcadastro.controle_processamento.dt_limite_inf_nf%type := NULL, p_dt_limite_inf_nf_fim gfcadastro.controle_processamento.dt_limite_inf_nf%type := NULL, p_rowid_cp rowid := NULL, p_st_processamento gfcadastro.controle_processamento.st_processamento%type := NULL )
		IS
		SELECT
		cp.rowid             AS ROWID_CP,
		cp.qt_atualizados_nf AS QT_PROCESSADOS,
		cp.*,
		CAST(0 AS NUMBER)                     AS UPDATE_REG,
		NVL(TRIM(cp.DS_FILTRO),'1=1')         AS FILTRO,
		NVL(TRIM(cp.DS_TRANSACAO),'ROLLBACK') AS TRANSACAO,
		UPPER(TRIM(TRANSLATE(REPLACE(','
		||NVL(TRIM(REPLACE(cp.DS_REGRAS,'"','')),'N/A')
		|| ',','''',''),'x ','x')))   AS REGRAS,
		TRIM(cp.DS_OUTROS_PARAMETROS) AS OUTROS_PARAMETROS
		FROM ${TABELA_CONTROLE} cp
		WHERE ((p_rowid_cp IS NULL OR NVL(LENGTH(TRIM(p_rowid_cp)),0) = 0) OR (cp.rowid = p_rowid_cp)) 
		AND ((p_nm_processo IS NULL OR NVL(LENGTH(TRIM(p_nm_processo)),0) = 0) OR (cp.nm_processo = p_nm_processo))
		AND ((p_dt_limite_inf_nf_ini IS NULL OR p_dt_limite_inf_nf_ini  < to_date('01/01/1980','DD/MM/YYYY')) OR (cp.dt_limite_inf_nf  >= p_dt_limite_inf_nf_ini))
		AND ((p_dt_limite_inf_nf_fim IS NULL OR p_dt_limite_inf_nf_fim < to_date('01/01/1980','DD/MM/YYYY')) OR (cp.dt_limite_inf_nf <= p_dt_limite_inf_nf_fim))
		AND ((p_st_processamento IS NULL OR NVL(LENGTH(TRIM(p_st_processamento)),0) = 0) OR (cp.st_processamento = p_st_processamento));
		v_cp c_cp%ROWTYPE;
		
		CURSOR c_st ( p_emps_cod openrisow.servico_telcom.emps_cod%TYPE
			, p_fili_cod openrisow.servico_telcom.fili_cod%TYPE
			, p_servtl_cod openrisow.servico_telcom.servtl_cod%TYPE
			, p_servtl_dat_atua openrisow.servico_telcom.servtl_dat_atua%TYPE
			)
		IS
		WITH TMP_F AS  (SELECT * FROM openrisow.filial f WHERE f.emps_cod   = p_emps_cod AND f.fili_cod   = p_fili_cod)
		, TMP AS (
			SELECT /*+ INDEX(st SERVICO_TELCOMP1) */
				   st.rowid as ROWID_ST
				,  st.*
				,  ROW_NUMBER() OVER(PARTITION BY 
										st.emps_cod,
										st.fili_cod,
										st.servtl_cod 
									ORDER BY
										(CASE WHEN f.fili_cod_insest = TMP_F.fili_cod_insest THEN 0 ELSE 1 END),
									    (CASE WHEN st.emps_cod = p_emps_cod THEN 0 ELSE 1 END), 
										(CASE WHEN st.fili_cod = p_fili_cod THEN 0 ELSE 1 END),
									    (CASE WHEN st.servtl_dat_atua-p_servtl_dat_atua <= 0 THEN 0	ELSE 1 END), 
										ABS(st.servtl_dat_atua-p_servtl_dat_atua)
					) nro_lnh
				,	(CASE WHEN st.servtl_dat_atua > p_servtl_dat_atua THEN p_servtl_dat_atua ELSE st.servtl_dat_atua END) servtl_dat_atual		
				,   (CASE WHEN f.fili_cod_insest = TMP_F.fili_cod_insest THEN 0 ELSE 1 END) fl_fili_cod_insest 
				,   (CASE WHEN st.emps_cod = p_emps_cod THEN 0 ELSE 1 END) fl_emps_cod
				,   (CASE WHEN st.fili_cod = p_fili_cod THEN 0 ELSE 1 END) fl_fili_cod
				,   (CASE WHEN st.servtl_dat_atua-p_servtl_dat_atua <= 0 THEN 0	ELSE 1 END) qt_dia_dt 
				,   ABS(st.servtl_dat_atua-p_servtl_dat_atua) qt_dif_dt
				
			FROM    openrisow.servico_telcom st
			      , openrisow.filial f
				  , TMP_F
			WHERE 	st.servtl_cod = p_servtl_cod
			AND (   EXISTS (SELECT /*+ FIRST_ROWS(1) */ 1 
			                FROM openrisow.servico_telcom st1 
							WHERE st1.servtl_cod = st.servtl_cod
							AND   st1.servtl_desc = st.servtl_desc
							AND   st1.emps_cod   = p_emps_cod
							AND   st1.fili_cod   = p_fili_cod
							)
				OR  EXISTS (SELECT /*+ FIRST_ROWS(1) */ 1 
			                FROM openrisow.servico_telcom st1, openrisow.filial f1 
							WHERE f1.fili_cod_insest IN (SELECT  /*+ FIRST_ROWS(1) */ f2.fili_cod_insest 
														FROM openrisow.filial f2 
														WHERE f2.emps_cod   = p_emps_cod
														AND   f2.fili_cod   = p_fili_cod
														)
							AND   st1.emps_cod   	= f1.emps_cod
							AND   st1.fili_cod   	= f1.fili_cod
							AND   st1.servtl_cod 	= st.servtl_cod
							AND   st1.servtl_desc = st.servtl_desc
							)
				OR 	EXISTS (SELECT /*+ FIRST_ROWS(1) */ 1 
			                FROM openrisow.servico_telcom st1 
							WHERE st1.servtl_cod = st.servtl_cod
							AND   st1.servtl_desc = st.servtl_desc
							)			
				)
			AND   f.emps_cod   	= st.emps_cod
			AND   f.fili_cod   	= st.fili_cod
											
			ORDER BY
					fl_fili_cod_insest
				,   fl_emps_cod
				,   fl_fili_cod
				,   qt_dia_dt
				,   qt_dif_dt	
		)		
		SELECT /*+ PARALLEL (8) */
				TMP.*
			,	CAST(0 AS NUMBER) AS update_reg -- 0 -- nao alterado , 1 - alterado
			,	CAST(0 AS NUMBER) AS sit_reg    -- 0 : null 1 - aberto 3 - n tem dado	  
			,   CAST(' ' AS VARCHAR2(4000)) AS rules
		FROM  TMP
		WHERE TMP.nro_lnh = 1 
		AND   ROWNUM < 2;   

        		  
		 
		
				
		c_st_i VARCHAR2(150);	
		TYPE c_st_t IS TABLE OF c_st%ROWTYPE INDEX BY c_st_i%TYPE;	
		v_bk_st_t c_st_t;
		TYPE c_st_t2 IS TABLE OF c_st%ROWTYPE INDEX BY PLS_INTEGER;	
		v_bk_st_t2 c_st_t2;	
						
		TYPE c_st_t3 IS TABLE OF c_st%ROWTYPE INDEX BY c_st_i%TYPE;	
		v_bk_st_t3 c_st_t3;
		
		v_nro_st  PLS_INTEGER := 0;
		v_st_t c_st%ROWTYPE;
		v_idx c_st_i%TYPE;
		v_nro_idx PLS_INTEGER :=0;
		v_existe  PLS_INTEGER  :=0;
	
		CURSOR c_regra_57  
		IS
		SELECT * FROM openrisow.TB_DE_PARA_57;
		c_regra_57_i VARCHAR2(150);
		TYPE c_regra_57_t IS TABLE OF c_regra_57%ROWTYPE INDEX BY c_regra_57_i%TYPE;
		v_bk_regra_57     c_regra_57_t;
			
		CURSOR c_f
		IS
		SELECT * FROM openrisow.filial;	
		c_f_i VARCHAR2(150);
		TYPE c_f_t IS TABLE OF c_f%ROWTYPE INDEX BY c_f_i%TYPE;
		v_bk_f     c_f_t;
		v_f        c_f%ROWTYPE;	
		
		CURSOR c_base_class_fis
		IS
		SELECT tb.* FROM gfcarga.tsh_tab_tp_utiliz_class_fis tb ORDER BY 1;
		TYPE t_TYPE_class_fis IS TABLE OF c_base_class_fis%ROWTYPE INDEX BY VARCHAR2(30);
		v_bk_class_fis    t_TYPE_class_fis;
		v_base_class_fis  c_base_class_fis%ROWTYPE;
		
		CURSOR c_cli( p_cadg_cod openrisow.cli_fornec_transp.cadg_cod%TYPE, 
					p_catg_cod openrisow.cli_fornec_transp.catg_cod%TYPE, 
					p_cadg_dat_atua openrisow.cli_fornec_transp.cadg_dat_atua%TYPE)
		IS
		WITH tmp AS
		(SELECT
			cli.rowid rowid_cli,
			cli.catg_cod   || '|' || cli.cadg_cod AS cli,  
			cli.cadg_cod as cadg_cod_new_cli,
			cli.cadg_cod as cadg_cod_cli,
			cli.catg_cod as catg_cod_cli,
			cli.cadg_dat_atua,
			cli.cadg_dat_atua cadg_dat_atua_new,
			cli.cadg_cod_cgccpf,
			cli.cadg_cod_insest,
			-- cli.cadg_tel,
			-- cli.cadg_ddd_tel,
			-- cli.pais_cod as pais_cod_cli,
			-- cli.loca_cod as loca_cod_cli,
			cli.unfe_sig as unfe_sig_cli,
			cli.mibge_cod_mun as mibge_cod_mun_cli,
			cli.cadg_end_munic as cadg_end_munic_cli,
			cli.cadg_end_cep,
			cli.cadg_end_bairro,
			-- cli.cadg_end_comp,
			-- cli.cadg_end_num,
			-- cli.cadg_end,
			-- cli.cadg_nom_fantasia,
			-- cli.cadg_nom,
			cli.cadg_tip,
			-- cli.tp_loc as tp_loc_cli,
			cli.var05 as var05_cli,
			cli.var05 as var05_comp,
			cli.var05 as var05_cli_new,
			row_number() over(partition BY cli.cadg_cod,cli.catg_cod order by cli.cadg_dat_atua DESC) nro_lnh
		FROM openrisow.cli_fornec_transp cli
		WHERE cli.cadg_cod     = p_cadg_cod
		AND cli.catg_cod       = p_catg_cod
		AND cli.cadg_dat_atua <= p_cadg_dat_atua
		)
		SELECT 
		tmp.*,
		comp.rowid rowid_comp,
		comp.cadg_num_conta,
		comp.cadg_num_conta as cadg_num_conta_new,
		comp.cadg_tip_assin,
		comp.cadg_tip_cli,
		comp.cadg_dat_atua cadg_dat_atua_new_comp,
		comp.cadg_dat_atua cadg_dat_atua_comp,
		comp.cadg_cod      cadg_cod_new_comp,
		comp.cadg_cod      cadg_cod_comp,
		comp.catg_cod      catg_cod_comp,	  
		comp.cadg_uf_habilit,
		comp.cadg_grp_tensao,
		comp.cadg_tip_utiliz,
		comp.cadg_tel_contato,
		CAST(0 AS NUMBER) 		 AS insere_reg, -- 0 -- nao insere , 1 - insere
		CAST(0 AS NUMBER) 		 AS insere_reg_comp, -- 0 -- nao insere , 1 - insere
		CAST(0 AS NUMBER) 		 AS update_reg, -- 0 -- nao alterado , 1 - alterado
		CAST(0 AS NUMBER) 		 AS update_reg_comp, -- 0 -- nao alterado , 1 - alterado
		CAST(0 AS NUMBER)          AS sit_reg -- 0 : null 1 - aberto 3 - n tem dado
	,  CAST(' ' AS VARCHAR2(4000)) AS rules	
	,  CAST(' ' AS VARCHAR2(4000)) AS rules_comp
		FROM openrisow.complvu_clifornec comp,
		tmp
		WHERE tmp.nro_lnh      = 1
		AND comp.cadg_cod      = tmp.cadg_cod_cli
		AND comp.catg_cod      = tmp.catg_cod_cli
		AND comp.cadg_dat_atua = tmp.cadg_dat_atua;
		TYPE t_cli IS TABLE OF c_cli%ROWTYPE INDEX BY PLS_INTEGER;
		v_bk_cli  t_cli;	
		v_bk_comp t_cli;	
		v_bk_cli_ins  t_cli;	
		v_bk_comp_ins t_cli;		
		v_cli     c_cli%ROWTYPE;
		-- 387
		CURSOR c_inf(p_emps_cod         IN openrisow.item_nftl_serv.emps_cod%type,
					p_fili_cod         IN openrisow.item_nftl_serv.fili_cod%type,
					p_infst_dtemiss    IN openrisow.item_nftl_serv.infst_dtemiss%type,
					p_infst_serie      IN openrisow.item_nftl_serv.infst_serie%type,
					p_infst_num        IN openrisow.item_nftl_serv.infst_num%type)
		IS
		SELECT
		inf.rowid rowid_inf,
		inf.*,
		'N' AS last_reg_nf,
		'N' AS last_reg_cli,	  
		upper(trim(TRANSLATE(inf.infst_serie,'x ','x'))) serie,
		NULLIF(ROW_NUMBER() over(PARTITION BY inf.emps_cod,inf.fili_cod,inf.infst_dtemiss,inf.infst_serie,inf.infst_num ORDER BY
			CASE inf.infst_num_seq
			WHEN 0
			THEN NULL
			ELSE inf.infst_num_seq
		END NULLs LAST),inf.infst_num_seq) AS infst_num_seq_aux,
		MAX(inf.infst_num_seq) OVER (PARTITION BY inf.emps_cod,inf.fili_cod,inf.infst_dtemiss,inf.infst_serie,inf.infst_num) AS infst_num_seq_max,
		CAST(0 AS NUMBER) AS update_reg, -- 0 -- nao alterado , 1 - alterado
		CAST(0 AS NUMBER) AS sit_reg -- 0 : null 1 - aberto 3 - n tem dado
	,  CAST(' ' AS VARCHAR2(4000)) AS rules
		FROM  openrisow.item_nftl_serv ${PARTITION_INF} inf   
		WHERE inf.emps_cod            = p_emps_cod
		AND   inf.fili_cod            = p_fili_cod
		AND   inf.infst_dtemiss       = p_infst_dtemiss
		AND   inf.infst_serie         = p_infst_serie
		AND   inf.infst_num           = p_infst_num;	
		TYPE t_inf IS TABLE OF c_inf%ROWTYPE INDEX BY PLS_INTEGER;
		v_bk_inf     t_inf;
		v_inf 	     c_inf%rowtype;
		v_inf_aux 	 c_inf%rowtype;
		v_bk_c_inf   t_inf;
		c_37_i       VARCHAR2(150);
		TYPE c_37_t IS TABLE OF c_inf%ROWTYPE INDEX BY c_37_i%TYPE;
		v_bk_c_37_t        c_37_t;	
		v_nr_limite_nf_37  NUMBER := 0;
	
							
		CURSOR c_nf( p_rowid_nf IN ROWID)
		IS
		SELECT
		nf.rowid rowid_nf,
		nf.*,
		nf.mnfst_tip_util AS tipo_utilizacao,
		nf.mnfst_ind_cont AS mnfst_ind_cont_aux,
		upper(trim(TRANSLATE(nf.mnfst_serie,'x ','x'))) serie,
		'N' AS last_reg_nf,
		'N' AS last_reg_cli,
		CAST(0 AS NUMBER) AS update_reg, -- 0 -- não alterado , 1 - alterado
		CAST(0 AS NUMBER) AS sit_reg -- 0 : null 1 - aberto 3 - n tem dado
	,  CAST(' ' AS VARCHAR2(4000)) AS rules	  	
		FROM openrisow.mestre_nftl_serv  ${PARTITION_NF} nf  
		WHERE   nf.ROWID = p_rowid_nf; 
		TYPE t_nf IS TABLE OF c_nf%ROWTYPE INDEX BY PLS_INTEGER;
		v_bk_nf    t_nf;
		v_nf 	   c_nf%rowtype;
	
	
		PROCEDURE prc_tmp_inf_find_insert(p_inf_find IN c_inf%rowtype, p_inf_insert IN OUT NOCOPY c_inf%rowtype) AS
		BEGIN
			
			p_inf_insert                     := p_inf_find;		
			p_inf_insert.infst_num_seq       := p_inf_insert.infst_num_seq_max;
			p_inf_insert.update_reg          := 0;	
			p_inf_insert.infst_num_seq_aux   := NULL;
			p_inf_insert.last_reg_nf         := 'N';
			p_inf_insert.last_reg_cli        := 'N';	
			
			SELECT
				rowid as rowid_inf        ,
				inf.emps_cod              , 
				inf.fili_cod              , 
				inf.cgc_cpf               , 
				inf.ie                    , 
				inf.uf                    , 
				inf.tp_loc                , 
				inf.localidade            , 
				inf.tdoc_cod              , 
				inf.infst_serie           , 
				inf.infst_num             , 
				inf.infst_dtemiss         , 
				inf.catg_cod              , 
				inf.cadg_cod              , 
				inf.serv_cod              , 
				inf.estb_cod              , 
				inf.infst_dsc_compl       , 
				inf.infst_val_cont        , 
				inf.infst_val_serv        , 
				inf.infst_val_desc        , 
				inf.infst_aliq_icms       , 
				inf.infst_base_icms       , 
				inf.infst_val_icms        , 
				inf.infst_isenta_icms     , 
				inf.infst_outras_icms     , 
				inf.infst_tribipi         , 
				inf.infst_tribicms        , 
				inf.infst_isenta_ipi      , 
				inf.infst_outra_ipi       , 
				inf.infst_outras_desp     , 
				inf.infst_fiscal          , 
				p_inf_insert.infst_num_seq_max AS infst_num_seq        ,
				p_inf_insert.infst_num_seq_max AS infst_num_seq_max    , 			
				inf.infst_tel             , 
				inf.infst_ind_canc        , 
				inf.infst_proter          , 
				inf.infst_cod_cont        , 
				inf.cfop                  , 
				inf.mdoc_cod              , 
				inf.cod_prest             , 
				inf.num01                 , 
				inf.num02                 , 
				inf.num03                 , 
				inf.var01                 , 
				inf.var02                 , 
				inf.var03                 , 
				inf.var04                 , 
				inf.var05                 , 
				inf.infst_ind_cnv115      , 
				inf.infst_unid_medida     , 
				inf.infst_quant_contr     , 
				inf.infst_quant_prest     , 
				inf.infst_codh_reg        , 
				inf.esta_cod              , 
				inf.infst_val_pis         , 
				inf.infst_val_cofins      , 
				inf.infst_bas_icms_st     , 
				inf.infst_aliq_icms_st    , 
				inf.infst_val_icms_st     , 
				inf.infst_val_red         , 
				inf.tpis_cod              , 
				inf.tcof_cod              , 
				inf.infst_bas_piscof      , 
				inf.infst_aliq_pis        , 
				inf.infst_aliq_cofins     , 
				inf.infst_nat_rec         , 
				inf.cscp_cod              , 
				inf.infst_num_contr       , 
				inf.infst_tip_isencao     , 
				inf.infst_tar_aplic       , 
				inf.infst_ind_desc        , 
				inf.infst_num_fat         , 
				inf.infst_qtd_fat         , 
				inf.infst_mod_ativ        , 
				inf.infst_hora_ativ       , 
				inf.infst_id_equip        , 
				inf.infst_mod_pgto        , 
				inf.infst_num_nfe         , 
				inf.infst_dtemiss_nfe     , 
				inf.infst_val_cred_nfe    , 
				inf.infst_cnpj_can_com    , 
				inf.infst_val_desc_pis    , 
				inf.infst_val_desc_cofins ,
				inf.infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				inf.infst_fcp_st	        -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
			INTO		
				p_inf_insert.rowid_inf             ,
				p_inf_insert.emps_cod              , 
				p_inf_insert.fili_cod              , 
				p_inf_insert.cgc_cpf               , 
				p_inf_insert.ie                    , 
				p_inf_insert.uf                    , 
				p_inf_insert.tp_loc                , 
				p_inf_insert.localidade            , 
				p_inf_insert.tdoc_cod              , 
				p_inf_insert.infst_serie           , 
				p_inf_insert.infst_num             , 
				p_inf_insert.infst_dtemiss         , 
				p_inf_insert.catg_cod              , 
				p_inf_insert.cadg_cod              , 
				p_inf_insert.serv_cod              , 
				p_inf_insert.estb_cod              , 
				p_inf_insert.infst_dsc_compl       , 
				p_inf_insert.infst_val_cont        , 
				p_inf_insert.infst_val_serv        , 
				p_inf_insert.infst_val_desc        , 
				p_inf_insert.infst_aliq_icms       , 
				p_inf_insert.infst_base_icms       , 
				p_inf_insert.infst_val_icms        , 
				p_inf_insert.infst_isenta_icms     , 
				p_inf_insert.infst_outras_icms     , 
				p_inf_insert.infst_tribipi         , 
				p_inf_insert.infst_tribicms        , 
				p_inf_insert.infst_isenta_ipi      , 
				p_inf_insert.infst_outra_ipi       , 
				p_inf_insert.infst_outras_desp     , 
				p_inf_insert.infst_fiscal          , 
				p_inf_insert.infst_num_seq         , 
				p_inf_insert.infst_num_seq_max     ,
				p_inf_insert.infst_tel             , 
				p_inf_insert.infst_ind_canc        , 
				p_inf_insert.infst_proter          , 
				p_inf_insert.infst_cod_cont        , 
				p_inf_insert.cfop                  , 
				p_inf_insert.mdoc_cod              , 
				p_inf_insert.cod_prest             , 
				p_inf_insert.num01                 , 
				p_inf_insert.num02                 , 
				p_inf_insert.num03                 , 
				p_inf_insert.var01                 , 
				p_inf_insert.var02                 , 
				p_inf_insert.var03                 , 
				p_inf_insert.var04                 , 
				p_inf_insert.var05                 , 
				p_inf_insert.infst_ind_cnv115      , 
				p_inf_insert.infst_unid_medida     , 
				p_inf_insert.infst_quant_contr     , 
				p_inf_insert.infst_quant_prest     , 
				p_inf_insert.infst_codh_reg        , 
				p_inf_insert.esta_cod              , 
				p_inf_insert.infst_val_pis         , 
				p_inf_insert.infst_val_cofins      , 
				p_inf_insert.infst_bas_icms_st     , 
				p_inf_insert.infst_aliq_icms_st    , 
				p_inf_insert.infst_val_icms_st     , 
				p_inf_insert.infst_val_red         , 
				p_inf_insert.tpis_cod              , 
				p_inf_insert.tcof_cod              , 
				p_inf_insert.infst_bas_piscof      , 
				p_inf_insert.infst_aliq_pis        , 
				p_inf_insert.infst_aliq_cofins     , 
				p_inf_insert.infst_nat_rec         , 
				p_inf_insert.cscp_cod              , 
				p_inf_insert.infst_num_contr       , 
				p_inf_insert.infst_tip_isencao     , 
				p_inf_insert.infst_tar_aplic       , 
				p_inf_insert.infst_ind_desc        , 
				p_inf_insert.infst_num_fat         , 
				p_inf_insert.infst_qtd_fat         , 
				p_inf_insert.infst_mod_ativ        , 
				p_inf_insert.infst_hora_ativ       , 
				p_inf_insert.infst_id_equip        , 
				p_inf_insert.infst_mod_pgto        , 
				p_inf_insert.infst_num_nfe         , 
				p_inf_insert.infst_dtemiss_nfe     , 
				p_inf_insert.infst_val_cred_nfe    , 
				p_inf_insert.infst_cnpj_can_com    , 
				p_inf_insert.infst_val_desc_pis    , 
				p_inf_insert.infst_val_desc_cofins ,
				p_inf_insert.infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				p_inf_insert.infst_fcp_st	         -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185				
			FROM  openrisow.item_nftl_serv inf	
			WHERE 	inf.ROWID = p_inf_find.rowid_inf;	
		END;
		
		PROCEDURE prc_tmp_inf_insert(p_inf_insert IN OUT NOCOPY c_inf%rowtype) AS
		BEGIN
			
			INSERT INTO openrisow.item_nftl_serv 
			(
				emps_cod              , 
				fili_cod              , 
				cgc_cpf               , 
				ie                    , 
				uf                    , 
				tp_loc                , 
				localidade            , 
				tdoc_cod              , 
				infst_serie           , 
				infst_num             , 
				infst_dtemiss         , 
				catg_cod              , 
				cadg_cod              , 
				serv_cod              , 
				estb_cod              , 
				infst_dsc_compl       , 
				infst_val_cont        , 
				infst_val_serv        , 
				infst_val_desc        , 
				infst_aliq_icms       , 
				infst_base_icms       , 
				infst_val_icms        , 
				infst_isenta_icms     , 
				infst_outras_icms     , 
				infst_tribipi         , 
				infst_tribicms        , 
				infst_isenta_ipi      , 
				infst_outra_ipi       , 
				infst_outras_desp     , 
				infst_fiscal          , 
				infst_num_seq         , 
				infst_tel             , 
				infst_ind_canc        , 
				infst_proter          , 
				infst_cod_cont        , 
				cfop                  , 
				mdoc_cod              , 
				cod_prest             , 
				num01                 , 
				num02                 , 
				num03                 , 
				var01                 , 
				var02                 , 
				var03                 , 
				var04                 , 
				var05                 , 
				infst_ind_cnv115      , 
				infst_unid_medida     , 
				infst_quant_contr     , 
				infst_quant_prest     , 
				infst_codh_reg        , 
				esta_cod              , 
				infst_val_pis         , 
				infst_val_cofins      , 
				infst_bas_icms_st     , 
				infst_aliq_icms_st    , 
				infst_val_icms_st     , 
				infst_val_red         , 
				tpis_cod              , 
				tcof_cod              , 
				infst_bas_piscof      , 
				infst_aliq_pis        , 
				infst_aliq_cofins     , 
				infst_nat_rec         , 
				cscp_cod              , 
				infst_num_contr       , 
				infst_tip_isencao     , 
				infst_tar_aplic       , 
				infst_ind_desc        , 
				infst_num_fat         , 
				infst_qtd_fat         , 
				infst_mod_ativ        , 
				infst_hora_ativ       , 
				infst_id_equip        , 
				infst_mod_pgto        , 
				infst_num_nfe         , 
				infst_dtemiss_nfe     , 
				infst_val_cred_nfe    , 
				infst_cnpj_can_com    , 
				infst_val_desc_pis    , 
				infst_val_desc_cofins ,
				infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				infst_fcp_st	        -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185 	  
			)
			VALUES
			(
				p_inf_insert.emps_cod              , 
				p_inf_insert.fili_cod              , 
				p_inf_insert.cgc_cpf               , 
				p_inf_insert.ie                    , 
				p_inf_insert.uf                    , 
				p_inf_insert.tp_loc                , 
				p_inf_insert.localidade            , 
				p_inf_insert.tdoc_cod              , 
				p_inf_insert.infst_serie           , 
				p_inf_insert.infst_num             , 
				p_inf_insert.infst_dtemiss         , 
				p_inf_insert.catg_cod              , 
				p_inf_insert.cadg_cod              , 
				p_inf_insert.serv_cod              , 
				p_inf_insert.estb_cod              , 
				p_inf_insert.infst_dsc_compl       , 
				p_inf_insert.infst_val_cont        , 
				p_inf_insert.infst_val_serv        , 
				p_inf_insert.infst_val_desc        , 
				p_inf_insert.infst_aliq_icms       , 
				p_inf_insert.infst_base_icms       , 
				p_inf_insert.infst_val_icms        , 
				p_inf_insert.infst_isenta_icms     , 
				p_inf_insert.infst_outras_icms     , 
				p_inf_insert.infst_tribipi         , 
				p_inf_insert.infst_tribicms        , 
				p_inf_insert.infst_isenta_ipi      , 
				p_inf_insert.infst_outra_ipi       , 
				p_inf_insert.infst_outras_desp     , 
				p_inf_insert.infst_fiscal          , 
				p_inf_insert.infst_num_seq         , 
				p_inf_insert.infst_tel             , 
				p_inf_insert.infst_ind_canc        , 
				p_inf_insert.infst_proter          , 
				p_inf_insert.infst_cod_cont        , 
				p_inf_insert.cfop                  , 
				p_inf_insert.mdoc_cod              , 
				p_inf_insert.cod_prest             , 
				p_inf_insert.num01                 , 
				p_inf_insert.num02                 , 
				p_inf_insert.num03                 , 
				p_inf_insert.var01                 , 
				p_inf_insert.var02                 , 
				p_inf_insert.var03                 , 
				p_inf_insert.var04                 , 
				p_inf_insert.var05                 , 
				p_inf_insert.infst_ind_cnv115      , 
				p_inf_insert.infst_unid_medida     , 
				p_inf_insert.infst_quant_contr     , 
				p_inf_insert.infst_quant_prest     , 
				p_inf_insert.infst_codh_reg        , 
				p_inf_insert.esta_cod              , 
				p_inf_insert.infst_val_pis         , 
				p_inf_insert.infst_val_cofins      , 
				p_inf_insert.infst_bas_icms_st     , 
				p_inf_insert.infst_aliq_icms_st    , 
				p_inf_insert.infst_val_icms_st     , 
				p_inf_insert.infst_val_red         , 
				p_inf_insert.tpis_cod              , 
				p_inf_insert.tcof_cod              , 
				p_inf_insert.infst_bas_piscof      , 
				p_inf_insert.infst_aliq_pis        , 
				p_inf_insert.infst_aliq_cofins     , 
				p_inf_insert.infst_nat_rec         , 
				p_inf_insert.cscp_cod              , 
				p_inf_insert.infst_num_contr       , 
				p_inf_insert.infst_tip_isencao     , 
				p_inf_insert.infst_tar_aplic       , 
				p_inf_insert.infst_ind_desc        , 
				p_inf_insert.infst_num_fat         , 
				p_inf_insert.infst_qtd_fat         , 
				p_inf_insert.infst_mod_ativ        , 
				p_inf_insert.infst_hora_ativ       , 
				p_inf_insert.infst_id_equip        , 
				p_inf_insert.infst_mod_pgto        , 
				p_inf_insert.infst_num_nfe         , 
				p_inf_insert.infst_dtemiss_nfe     , 
				p_inf_insert.infst_val_cred_nfe    , 
				p_inf_insert.infst_cnpj_can_com    , 
				p_inf_insert.infst_val_desc_pis    , 
				p_inf_insert.infst_val_desc_cofins ,
				p_inf_insert.infst_fcp_pro         , -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				p_inf_insert.infst_fcp_st	         -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
				) RETURNING ROWID INTO p_inf_insert.rowid_inf;			
		END;
		
		-- Especificacao de função a ser utilizado para todas
		PROCEDURE prcts_tratar_inf(
			p_cli1            IN OUT NOCOPY  c_cli%ROWTYPE,
			p_f1              IN c_f%ROWTYPE,
			p_inf1            IN OUT NOCOPY  c_inf%rowtype,
			p_nf1             IN OUT NOCOPY  c_nf%rowtype,
			p_sanea1          IN OUT NOCOPY  c_sanea%ROWTYPE,
			p_cp1             IN c_cp%ROWTYPE,   
			p_st_t1           IN OUT NOCOPY  c_st%ROWTYPE,	
			p_nr_qtde_inf1    IN OUT NOCOPY  PLS_INTEGER);	
	
		PROCEDURE prcts_tratar_nf(
			p_cli1            IN OUT NOCOPY  c_cli%ROWTYPE,
			p_f1              IN c_f%ROWTYPE,
			p_inf1            IN c_inf%rowtype,
			p_nf1             IN OUT NOCOPY  c_nf%rowtype,
			p_sanea1          IN c_sanea%ROWTYPE,
			p_cp1             IN c_cp%ROWTYPE);
			
		PROCEDURE prcts_tratar_cli(
			p_cli1            IN OUT NOCOPY  c_cli%ROWTYPE,
			p_f1              IN c_f%ROWTYPE,
			p_inf1            IN c_inf%rowtype,
			p_nf1             IN c_nf%rowtype,
			p_sanea1          IN c_sanea%ROWTYPE,
			p_cp1             IN c_cp%ROWTYPE);		
		
	
		-- Regras (tem que ser ultimos a ser declarados na sequencia abaixo)
		${ARQUIVOS_SCRIPTS}
		-- 
	
		-- Tratamento da funcao prcts_tratar_inf	
		-- CREATE OR REPLACE 
		PROCEDURE prcts_tratar_inf(
			p_cli1            IN OUT NOCOPY  c_cli%ROWTYPE,
			p_f1              IN c_f%ROWTYPE,
			p_inf1            IN OUT NOCOPY  c_inf%rowtype,
			p_nf1             IN OUT NOCOPY  c_nf%rowtype,
			p_sanea1          IN OUT NOCOPY  c_sanea%ROWTYPE,
			p_cp1             IN c_cp%ROWTYPE,   
			p_st_t1           IN OUT NOCOPY  c_st%ROWTYPE,	
			p_nr_qtde_inf1    IN OUT NOCOPY  PLS_INTEGER)
		AS
		BEGIN
			-- 
			-- 1950
			${PRCTS_TRATAR_INF}   
		
			IF NVL(p_inf1.update_reg,0) != 0 AND p_inf1.rowid_inf IS NOT NULL THEN
				prcts_etapa('add bulk inf >> ' || p_inf1.rowid_inf|| ' ');
				v_bk_inf(nvl(v_bk_inf.COUNT,0)+1) := p_inf1;
				:v_qtd_atu_inf   := :v_qtd_atu_inf + 1;
				p_inf1.update_reg := 0;
				prcts_process_log(p_nm_var01      =>  p_inf1.emps_cod,
								p_nm_var02      =>  p_inf1.fili_cod,
								p_nm_var03      =>  p_inf1.infst_serie,
								p_nm_var04      =>  TO_CHAR(p_inf1.infst_dtemiss,'YYYY-MM-DD'),
								p_nm_var05      =>  CONSTANTE_AMOUNT,
								p_nr_var02      =>  1); 	
			END IF;		
			v_inf_rules := NULL;					
				
			IF NVL(p_st_t1.update_reg,0) != 0 THEN
				prcts_etapa('add bulk st >> ' || p_st_t1.rowid_st|| ' ');
				v_bk_st_t(p_st_t1.EMPS_COD|| '|'||p_st_t1.FILI_COD|| '|'||TO_CHAR(p_st_t1.SERVTL_DAT_ATUA,'YYYYMMDD')|| '|'||p_st_t1.SERVTL_COD) := p_st_t1;
				:v_qtd_atu_st := :v_qtd_atu_st + 1;
				p_st_t1.update_reg := 0;
				prcts_process_log(p_nm_var01      =>  p_inf1.emps_cod,
								p_nm_var02      =>  p_inf1.fili_cod,
								p_nm_var03      =>  p_inf1.infst_serie,
								p_nm_var04      =>  TO_CHAR(p_inf1.infst_dtemiss,'YYYY-MM-DD'),
								p_nm_var05      =>  CONSTANTE_AMOUNT,
								p_nr_var05      =>  1); 
			END IF;		
			v_st_t_rules := NULL;
			
		END;
		-- /
		-- CREATE OR REPLACE 
		PROCEDURE prcts_tratar_nf(
				p_cli1            IN OUT NOCOPY  c_cli%ROWTYPE,
				p_f1              IN c_f%ROWTYPE,
				p_inf1            IN c_inf%rowtype,
				p_nf1             IN OUT NOCOPY  c_nf%rowtype,
				p_sanea1          IN c_sanea%ROWTYPE,
				p_cp1             IN c_cp%ROWTYPE)
		AS
		BEGIN
		
			${PRCTS_TRATAR_NF}   
			
			IF NVL(p_nf1.update_reg,0) != 0 AND p_nf1.rowid_nf IS NOT NULL THEN
				prcts_etapa('add bulk nf >> ' || p_nf1.rowid_nf || ' ');
				v_bk_nf(nvl(v_bk_nf.COUNT,0)+1) := p_nf1;
				:v_qtd_atu_nf    := :v_qtd_atu_nf + 1;
				p_nf1.update_reg  := 0;
				prcts_process_log(p_nm_var01      =>  p_nf1.emps_cod,
								p_nm_var02      =>  p_nf1.fili_cod,
								p_nm_var03      =>  p_nf1.mnfst_serie,
								p_nm_var04      =>  TO_CHAR(p_nf1.mnfst_dtemiss,'YYYY-MM-DD'),
								p_nm_var05      =>  CONSTANTE_AMOUNT,
								p_nr_var01      =>  1);
			END IF;	
			v_nf_rules := NULL;	
			
			IF TRIM(v_cli_rules) IS NOT NULL THEN
				prcts_process_log(p_nm_var01      =>  p_nf1.emps_cod,
								p_nm_var02      =>  p_nf1.fili_cod,
								p_nm_var03      =>  p_nf1.mnfst_serie,
								p_nm_var04      =>  TO_CHAR(p_nf1.mnfst_dtemiss,'YYYY-MM-DD'),
								p_nm_var05      =>  CONSTANTE_AMOUNT,
								p_nr_var03      =>  1); 	
				v_cli_rules := NULL;	
			END IF;	
		
			IF TRIM(v_cli_rules_comp) IS NOT NULL THEN
				prcts_process_log(p_nm_var01      =>  p_nf1.emps_cod,
								p_nm_var02      =>  p_nf1.fili_cod,
								p_nm_var03      =>  p_nf1.mnfst_serie,
								p_nm_var04      =>  TO_CHAR(p_nf1.mnfst_dtemiss,'YYYY-MM-DD'),
								p_nm_var05      =>  CONSTANTE_AMOUNT,
								p_nr_var04      =>  1); 	
				v_cli_rules_comp := NULL;	
			END IF;	
		
			IF NVL(p_cli1.insere_reg,0) != 0 AND p_cli1.rowid_cli IS NOT NULL THEN
				prcts_etapa('add bulk cli ins >> ' || p_cli1.rowid_cli || ' ');
				v_bk_cli_ins(nvl(v_bk_cli_ins.COUNT,0)+1) := p_cli1;
				p_cli1.insere_reg  := 0;	
			END IF;	
			
			IF NVL(p_cli1.insere_reg_comp,0) != 0 AND p_cli1.rowid_comp IS NOT NULL THEN
				prcts_etapa('add bulk comp ins >> ' || p_cli1.rowid_comp || ' ');
				v_bk_comp_ins(nvl(v_bk_comp_ins.COUNT,0)+1) := p_cli1;
				p_cli1.insere_reg_comp := 0;
			END IF;		
			
		END;
		
		-- /
		-- CREATE OR REPLACE 
		PROCEDURE prcts_tratar_cli(
				p_cli1            IN OUT NOCOPY  c_cli%ROWTYPE,
				p_f1              IN c_f%ROWTYPE,
				p_inf1            IN c_inf%rowtype,
				p_nf1             IN c_nf%rowtype,
				p_sanea1          IN c_sanea%ROWTYPE,
				p_cp1             IN c_cp%ROWTYPE)	
		AS
		BEGIN
		
			${PRCTS_TRATAR_CLI} 
		
			IF NVL(p_cli1.update_reg,0) != 0 AND p_cli1.rowid_cli IS NOT NULL THEN
				prcts_etapa('add bulk cli >> ' || p_cli1.rowid_cli || ' ');
				v_bk_cli(nvl(v_bk_cli.COUNT,0)+1) := p_cli1;
				:v_qtd_atu_cli     := :v_qtd_atu_cli + 1;
				p_cli1.update_reg  := 0;
				IF TRIM(v_cli_rules) IS NOT NULL THEN
					prcts_process_log(p_nm_var01    =>  p_nf1.emps_cod,
									p_nm_var02      =>  p_nf1.fili_cod,
									p_nm_var03      =>  p_nf1.mnfst_serie,
									p_nm_var04      =>  TO_CHAR(p_nf1.mnfst_dtemiss,'YYYY-MM-DD'),
									p_nm_var05      =>  CONSTANTE_AMOUNT,
									p_nr_var03      =>  1); 	
					
				END IF;	
			END IF;	
			v_cli_rules := NULL;	
			
			IF NVL(p_cli1.update_reg_comp,0) != 0 AND p_cli1.rowid_comp IS NOT NULL THEN
				prcts_etapa('add bulk comp >> ' || p_cli1.rowid_comp || ' ');
				v_bk_comp(nvl(v_bk_comp.COUNT,0)+1) := p_cli1;
				:v_qtd_atu_comp        := :v_qtd_atu_comp + 1;
				p_cli1.update_reg_comp  := 0;
				IF TRIM(v_cli_rules_comp) IS NOT NULL THEN
					prcts_process_log(p_nm_var01      =>  p_nf1.emps_cod,
									p_nm_var02      =>  p_nf1.fili_cod,
									p_nm_var03      =>  p_nf1.mnfst_serie,
									p_nm_var04      =>  TO_CHAR(p_nf1.mnfst_dtemiss,'YYYY-MM-DD'),
									p_nm_var05      =>  CONSTANTE_AMOUNT,
									p_nr_var04      =>  1); 	
				END IF;	
			END IF;	
			v_cli_rules_comp := NULL;	
				
		END;
		-- /
			
	BEGIN
			
		-----------------------------------------------------------------------------
		--> Nomeando o processo
		-----------------------------------------------------------------------------	
		DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
		DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);
			
		-- 2062;
		v_cp.rowid_cp                := '${ROWID_CP}';
		
		prcts_etapa('INICIO',TRUE);
		-- RETURN;
		--;
		
		-- Inicializacao
		prcts_etapa('Inicializacao');
		
		-- DML
		v_cli.update_reg       := 0;
		v_cli.update_reg_comp  := 0;
		v_nf.update_reg        := 0;
		v_inf.update_reg       := 0;
		v_st_t.update_reg      := 0;
		
		-- CHAVE
		v_cli.cli              := NULL;
		v_nf.rowid_nf          := NULL;
		v_inf.rowid_inf        := NULL;
		
		prcts_etapa('Memoria: ' || SUBSTR('${CARREGAR_DADOS_MEMORIA}',1,2000));
		${CARREGAR_DADOS_MEMORIA}
		
		-- Filial
		FOR i IN c_f LOOP
		v_bk_f(TO_CHAR(i.emps_cod)|| '|' ||TO_CHAR(i.fili_cod)) := i;
		END LOOP;
		
		FOR idx IN c_base_class_fis	LOOP
			v_bk_class_fis(TO_CHAR(idx.serie)||TO_CHAR(idx.modelo_nf)) := idx;
		END LOOP;
		-- CP
		prcts_etapa('CP >> ' || v_cp.rowid_cp, TRUE);  
		OPEN c_cp(p_ROWID_CP => v_cp.rowid_cp);
		FETCH c_cp INTO v_cp;
		IF c_cp%NOTFOUND THEN
			RAISE_APPLICATION_ERROR (-20343, 'Controle de Processamento nao encontrado!');
		END IF;
		CLOSE c_cp;
		${COMMIT_SCRIPT};
		
		v_action_name := substr(to_char(v_cp.dt_limite_inf_nf,'DD/MM/YYYY') || ' >> ' || v_action_name,1,32);
		DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);
		
		${PRCTS_STOP}
		
		prcts_etapa('SANEA ' || to_char(v_cp.dt_limite_inf_nf,'DD/MM/YYYY'),TRUE);
		OPEN c_sanea;
		LOOP
			FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
			:v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
		
			IF v_bk_sanea.COUNT > 0 THEN
		
				FOR i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST 
				LOOP
		
					v_nr_qtde                  := v_nr_qtde + 1;
					v_nr_qtde_inf              := v_nr_qtde_inf + 1; 
					
					v_inf.last_reg_nf          := v_bk_sanea(i).last_reg_nf;
					v_inf.last_reg_cli         := v_bk_sanea(i).last_reg_cli;
					v_inf_aux.last_reg_nf      := v_bk_sanea(i).last_reg_nf;
					v_inf_aux.last_reg_cli     := v_bk_sanea(i).last_reg_cli;
					v_nf.last_reg_nf           := v_bk_sanea(i).last_reg_nf;
					v_nf.last_reg_cli          := v_bk_sanea(i).last_reg_cli;
					
					-- Atribuicao AUX INF
					v_inf_aux.sit_reg                := 0;
					v_inf_aux.update_reg             := 0;
					v_inf_aux.rowid_inf              := v_bk_sanea(i).rowid_inf;
					v_inf_aux.infst_num_seq_aux      := NULL;
					
					-- Atribuicao INF
					v_inf.sit_reg                    := 0;
					v_inf.update_reg                 := 0;
					v_inf.infst_num_seq_aux          := v_bk_sanea(i).infst_num_seq_aux;
					v_inf.rowid_inf                  := v_bk_sanea(i).rowid_inf;	
					v_inf.serie                      := v_bk_sanea(i).serie                ;
					v_inf.emps_cod                   := v_bk_sanea(i).emps_cod             ;
					v_inf.fili_cod                   := v_bk_sanea(i).fili_cod             ;
					v_inf.cgc_cpf                    := v_bk_sanea(i).cgc_cpf              ;
					v_inf.ie                         := v_bk_sanea(i).ie                   ;
					v_inf.uf                         := v_bk_sanea(i).uf                   ;
					-- v_inf.tp_loc                     := v_bk_sanea(i).tp_loc               ;
					-- v_inf.localidade                 := v_bk_sanea(i).localidade           ;
					-- v_inf.tdoc_cod                   := v_bk_sanea(i).tdoc_cod             ;
					v_inf.infst_serie                := v_bk_sanea(i).infst_serie          ;
					v_inf.infst_num                  := v_bk_sanea(i).infst_num            ;
					v_inf.infst_dtemiss              := v_bk_sanea(i).infst_dtemiss        ;
					v_inf.catg_cod                   := v_bk_sanea(i).infst_catg_cod       ;
					v_inf.cadg_cod                   := v_bk_sanea(i).infst_cadg_cod       ;
					v_inf.serv_cod                   := v_bk_sanea(i).serv_cod             ;
					v_inf.estb_cod                   := v_bk_sanea(i).estb_cod             ;
					v_inf.infst_dsc_compl            := v_bk_sanea(i).infst_dsc_compl      ;
					v_inf.infst_val_cont             := v_bk_sanea(i).infst_val_cont       ;
					v_inf.infst_val_serv             := v_bk_sanea(i).infst_val_serv       ;
					v_inf.infst_val_desc             := v_bk_sanea(i).infst_val_desc       ;
					v_inf.infst_aliq_icms            := v_bk_sanea(i).infst_aliq_icms      ;
					v_inf.infst_base_icms            := v_bk_sanea(i).infst_base_icms      ;
					v_inf.infst_val_icms             := v_bk_sanea(i).infst_val_icms       ;
					v_inf.infst_isenta_icms          := v_bk_sanea(i).infst_isenta_icms    ;
					v_inf.infst_outras_icms          := v_bk_sanea(i).infst_outras_icms    ;
					-- v_inf.infst_tribipi              := v_bk_sanea(i).infst_tribipi        ;
					v_inf.infst_tribicms             := v_bk_sanea(i).infst_tribicms       ;
					-- v_inf.infst_isenta_ipi           := v_bk_sanea(i).infst_isenta_ipi     ;
					-- v_inf.infst_outra_ipi            := v_bk_sanea(i).infst_outra_ipi      ;
					-- v_inf.infst_outras_desp          := v_bk_sanea(i).infst_outras_desp    ;
					-- v_inf.infst_fiscal               := v_bk_sanea(i).infst_fiscal         ;
					v_inf.infst_num_seq              := v_bk_sanea(i).infst_num_seq        ;
					v_inf.infst_tel                  := v_bk_sanea(i).infst_tel            ;
					v_inf.infst_ind_canc             := v_bk_sanea(i).infst_ind_canc       ;
					-- v_inf.infst_proter               := v_bk_sanea(i).infst_proter         ;
					-- v_inf.infst_cod_cont             := v_bk_sanea(i).infst_cod_cont       ;
					v_inf.cfop                       := v_bk_sanea(i).cfop                 ;
					v_inf.mdoc_cod                   := v_bk_sanea(i).mdoc_cod             ;
					-- v_inf.cod_prest                  := v_bk_sanea(i).cod_prest            ;
					-- v_inf.num01                      := v_bk_sanea(i).num01                ;
					-- v_inf.num02                      := v_bk_sanea(i).num02                ;
					-- v_inf.num03                      := v_bk_sanea(i).num03                ;
					-- v_inf.var01                      := v_bk_sanea(i).var01                ;
					-- v_inf.var02                      := v_bk_sanea(i).var02                ;
					-- v_inf.var03                      := v_bk_sanea(i).var03                ;
					-- v_inf.var04                      := v_bk_sanea(i).var04                ;				
					-- v_inf.infst_ind_cnv115           := v_bk_sanea(i).infst_ind_cnv115     ;
					-- v_inf.infst_unid_medida          := v_bk_sanea(i).infst_unid_medida    ;
					-- v_inf.infst_quant_contr          := v_bk_sanea(i).infst_quant_contr    ;
					-- v_inf.infst_quant_prest          := v_bk_sanea(i).infst_quant_prest    ;
					-- v_inf.infst_codh_reg             := v_bk_sanea(i).infst_codh_reg       ;
					v_inf.esta_cod                   := v_bk_sanea(i).esta_cod             ;
					v_inf.infst_val_pis              := v_bk_sanea(i).infst_val_pis        ;
					v_inf.infst_val_cofins           := v_bk_sanea(i).infst_val_cofins     ;
					-- v_inf.infst_bas_icms_st          := v_bk_sanea(i).infst_bas_icms_st    ;
					-- v_inf.infst_aliq_icms_st         := v_bk_sanea(i).infst_aliq_icms_st   ;
					-- v_inf.infst_val_icms_st          := v_bk_sanea(i).infst_val_icms_st    ;
					v_inf.infst_val_red              := v_bk_sanea(i).infst_val_red        ;
					-- v_inf.tpis_cod                   := v_bk_sanea(i).tpis_cod             ;
					-- v_inf.tcof_cod                   := v_bk_sanea(i).tcof_cod             ;
					-- v_inf.infst_bas_piscof           := v_bk_sanea(i).infst_bas_piscof     ;
					-- v_inf.infst_aliq_pis             := v_bk_sanea(i).infst_aliq_pis       ;
					-- v_inf.infst_aliq_cofins          := v_bk_sanea(i).infst_aliq_cofins    ;
					-- v_inf.infst_nat_rec              := v_bk_sanea(i).infst_nat_rec        ;
					-- v_inf.cscp_cod                   := v_bk_sanea(i).cscp_cod             ;
					v_inf.infst_num_contr            := v_bk_sanea(i).infst_num_contr      ;
					-- v_inf.infst_tip_isencao          := v_bk_sanea(i).infst_tip_isencao    ;
					-- v_inf.infst_tar_aplic            := v_bk_sanea(i).infst_tar_aplic      ;
					-- v_inf.infst_ind_desc             := v_bk_sanea(i).infst_ind_desc       ;
					-- v_inf.infst_num_fat              := v_bk_sanea(i).infst_num_fat        ;
					-- v_inf.infst_qtd_fat              := v_bk_sanea(i).infst_qtd_fat        ;
					-- v_inf.infst_mod_ativ             := v_bk_sanea(i).infst_mod_ativ       ;
					-- v_inf.infst_hora_ativ            := v_bk_sanea(i).infst_hora_ativ      ;
					-- v_inf.infst_id_equip             := v_bk_sanea(i).infst_id_equip       ;
					-- v_inf.infst_mod_pgto             := v_bk_sanea(i).infst_mod_pgto       ;
					-- v_inf.infst_num_nfe              := v_bk_sanea(i).infst_num_nfe        ;
					-- v_inf.infst_dtemiss_nfe          := v_bk_sanea(i).infst_dtemiss_nfe    ;
					-- v_inf.infst_val_cred_nfe         := v_bk_sanea(i).infst_val_cred_nfe   ;
					-- v_inf.infst_cnpj_can_com         := v_bk_sanea(i).infst_cnpj_can_com   ;
					-- v_inf.infst_val_desc_pis         := v_bk_sanea(i).infst_val_desc_pis   ;
					-- v_inf.infst_val_desc_cofins      := v_bk_sanea(i).infst_val_desc_cofins;	
					v_inf.var05                      := v_bk_sanea(i).var05                ;
					-- Atribuicao ST
					v_st_t.update_reg          := 0;
					IF v_bk_sanea(i).rowid_st is null THEN
						v_st_t.sit_reg               := 3;
					ELSE 	
						v_st_t.sit_reg               := 1;
					END IF;
					v_st_t.rowid_st 				 := v_bk_sanea(i).rowid_st;
					v_st_t.servtl_dat_atual 		 := v_bk_sanea(i).servtl_dat_atual;
					v_st_t.servtl_dat_atua 		     := v_bk_sanea(i).servtl_dat_atua;
					v_st_t.emps_cod                  := v_bk_sanea(i).emps_cod;
					v_st_t.fili_cod                  := v_bk_sanea(i).fili_cod;
					v_st_t.servtl_cod                := v_bk_sanea(i).servtl_cod;
					v_st_t.clasfi_cod                := v_bk_sanea(i).clasfi_cod;
					v_st_t.servtl_desc               := v_bk_sanea(i).servtl_desc;
					v_st_t.servtl_compl              := v_bk_sanea(i).servtl_compl;
					v_st_t.servtl_ind_tprec          := v_bk_sanea(i).servtl_ind_tprec;
					v_st_t.servtl_ind_tpserv         := v_bk_sanea(i).servtl_ind_tpserv;
					v_st_t.servtl_cod_nat            := v_bk_sanea(i).servtl_cod_nat;				
					v_st_t.var01 					 := v_bk_sanea(i).servtl_var01;
					v_st_t.var02 					 := v_bk_sanea(i).servtl_var02;
					v_st_t.var03 					 := v_bk_sanea(i).servtl_var03;
					v_st_t.var04 					 := v_bk_sanea(i).servtl_var04;
					v_st_t.var05 					 := v_bk_sanea(i).servtl_var05;
					v_st_t.num01 					 := v_bk_sanea(i).servtl_num01;
					v_st_t.num02 					 := v_bk_sanea(i).servtl_num02;
					v_st_t.num03 					 := v_bk_sanea(i).servtl_num03;
					v_st_t.servtl_ind_rec            := v_bk_sanea(i).servtl_ind_rec;
					v_st_t.servtl_tip_utiliz		 := v_bk_sanea(i).servtl_tip_utiliz;
					${ATRIBUICAO_ST}
					IF v_nf.rowid_nf IS NULL OR v_nf.rowid_nf != v_bk_sanea(i).rowid_nf THEN
		
						v_bk_c_inf.delete;
						v_cli.insere_reg             := 0;
						v_cli.insere_reg_comp        := 0;								
						v_cli.cadg_cod_new_cli       := v_bk_sanea(i).cadg_cod_cli           ;
						v_cli.cadg_dat_atua_new      := v_bk_sanea(i).cadg_dat_atua          ;					  
						v_cli.cadg_cod_new_comp      := v_bk_sanea(i).cadg_cod_comp          ;
						v_cli.cadg_dat_atua_new_comp := v_bk_sanea(i).cadg_dat_atua_comp     ;
						v_cli.cadg_num_conta_new     := v_bk_sanea(i).cadg_num_conta         ;
						v_cli.var05_cli_new 	     := v_bk_sanea(i).var05_cli			    ;	
						
						-- Atribuicao NF
						v_nf.update_reg              := 0;
						v_nf.sit_reg                 := 0;
						v_nf.rowid_nf                := v_bk_sanea(i).rowid_nf;						
						v_inf.infst_num_seq_max      := v_bk_sanea(i).infst_num_seq_max;
						v_inf_aux.infst_num_seq_max  := v_inf.infst_num_seq_max;
						v_nr_qtde_inf                := 1;
						
						BEGIN
							v_f :=  v_bk_f(TO_CHAR(v_bk_sanea(i).emps_cod)|| '|' ||TO_CHAR(v_bk_sanea(i).fili_cod));
						EXCEPTION
						WHEN OTHERS THEN
							NULL;
						END;
						
						v_nf.serie                   := v_bk_sanea(i).serie                ;
						v_nf.emps_cod                := v_bk_sanea(i).emps_cod             ; -- mnfst_emps_cod       ;
						v_nf.fili_cod                := v_bk_sanea(i).fili_cod             ; -- mnfst_fili_cod       ;
						-- v_nf.tdoc_cod                := v_bk_sanea(i).mnfst_tdoc_cod       ;
						v_nf.mnfst_serie             := v_bk_sanea(i).infst_serie          ; -- mnfst_serie          ;
						v_nf.mnfst_num               := v_bk_sanea(i).infst_num            ; -- mnfst_num            ;
						v_nf.mnfst_dtemiss           := v_bk_sanea(i).infst_dtemiss        ; -- mnfst_dtemiss        ;
						v_nf.catg_cod                := v_bk_sanea(i).mnfst_catg_cod       ;
						v_nf.cadg_cod                := v_bk_sanea(i).mnfst_cadg_cod       ;
						v_nf.mnfst_ind_cont          := v_bk_sanea(i).mnfst_ind_cont       ;
						v_nf.mnfst_ind_cont_aux      := v_bk_sanea(i).mnfst_ind_cont_aux   ;
						v_nf.mdoc_cod                := v_bk_sanea(i).mnfst_mdoc_cod       ;
						v_nf.mnfst_val_tot           := v_bk_sanea(i).mnfst_val_tot        ;
						v_nf.mnfst_val_desc          := v_bk_sanea(i).mnfst_val_desc       ;
						v_nf.mnfst_ind_canc          := v_bk_sanea(i).mnfst_ind_canc       ;
						-- v_nf.mnfst_dat_venc          := v_bk_sanea(i).mnfst_dat_venc       ;
						v_nf.mnfst_per_ref           := v_bk_sanea(i).mnfst_per_ref        ;
						-- v_nf.mnfst_avista            := v_bk_sanea(i).mnfst_avista         ;
						-- v_nf.num01                   := v_bk_sanea(i).mnfst_num01          ;
						-- v_nf.num02                   := v_bk_sanea(i).mnfst_num02          ;
						-- v_nf.num03                   := v_bk_sanea(i).mnfst_num03          ;
						-- v_nf.var01                   := v_bk_sanea(i).mnfst_var01          ;
						-- v_nf.var02                   := v_bk_sanea(i).mnfst_var02          ;
						-- v_nf.var03                   := v_bk_sanea(i).mnfst_var03          ;
						-- v_nf.var04                   := v_bk_sanea(i).mnfst_var04          ;
						v_nf.var05                   := v_bk_sanea(i).mnfst_var05          ;
						-- v_nf.mnfst_ind_cnv115        := v_bk_sanea(i).mnfst_ind_cnv115     ;
						v_nf.cnpj_cpf                := v_bk_sanea(i).mnfst_cnpj_cpf       ;
						v_nf.mnfst_val_basicms       := v_bk_sanea(i).mnfst_val_basicms    ;
						v_nf.mnfst_val_icms          := v_bk_sanea(i).mnfst_val_icms       ;
						v_nf.mnfst_val_isentas       := v_bk_sanea(i).mnfst_val_isentas    ;
						v_nf.mnfst_val_outras        := v_bk_sanea(i).mnfst_val_outras     ;
						v_nf.mnfst_codh_nf           := v_bk_sanea(i).mnfst_codh_nf        ;
						-- v_nf.mnfst_codh_regnf        := v_bk_sanea(i).mnfst_codh_regnf     ;
						-- v_nf.mnfst_codh_regcli       := v_bk_sanea(i).mnfst_codh_regcli    ;
						-- v_nf.mnfst_reg_esp           := v_bk_sanea(i).mnfst_reg_esp        ;
						-- v_nf.mnfst_bas_icms_st       := v_bk_sanea(i).mnfst_bas_icms_st    ;
						-- v_nf.mnfst_val_icms_st       := v_bk_sanea(i).mnfst_val_icms_st    ;
						-- v_nf.mnfst_val_pis           := v_bk_sanea(i).mnfst_val_pis        ;
						-- v_nf.mnfst_val_cofins        := v_bk_sanea(i).mnfst_val_cofins     ;
						-- v_nf.mnfst_val_da            := v_bk_sanea(i).mnfst_val_da         ;
						v_nf.mnfst_val_ser           := v_bk_sanea(i).mnfst_val_ser        ;
						-- v_nf.mnfst_val_terc          := v_bk_sanea(i).mnfst_val_terc       ;
						-- v_nf.cicd_cod_inf            := v_bk_sanea(i).mnfst_cicd_cod_inf   ;
						-- v_nf.mnfst_tip_assi          := v_bk_sanea(i).mnfst_tip_assi       ;
						v_nf.mnfst_tip_util          := v_bk_sanea(i).mnfst_tip_util       ;
						-- v_nf.mnfst_grp_tens          := v_bk_sanea(i).mnfst_grp_tens       ;
						-- v_nf.mnfst_ind_extemp        := v_bk_sanea(i).mnfst_ind_extemp     ;
						-- v_nf.mnfst_dat_extemp        := v_bk_sanea(i).mnfst_dat_extemp     ;
						-- v_nf.mnfst_num_fic           := v_bk_sanea(i).mnfst_num_fic        ;
						-- v_nf.mnfst_dt_lt_ant         := v_bk_sanea(i).mnfst_dt_lt_ant      ;
						-- v_nf.mnfst_dt_lt_atu         := v_bk_sanea(i).mnfst_dt_lt_atu      ;					
						-- v_nf.mnfst_vl_tot_fat        := v_bk_sanea(i).mnfst_vl_tot_fat     ;
						-- v_nf.mnfst_chv_nfe           := v_bk_sanea(i).mnfst_chv_nfe        ;
						-- v_nf.mnfst_dat_aut_nfe       := v_bk_sanea(i).mnfst_dat_aut_nfe    ;
						-- v_nf.mnfst_val_desc_pis      := v_bk_sanea(i).mnfst_val_desc_pis   ;
						-- v_nf.mnfst_val_desc_cofins 	 := v_bk_sanea(i).mnfst_val_desc_cofins;	
						v_nf.mnfst_num_fat           := v_bk_sanea(i).mnfst_num_fat        ;					
						BEGIN
							v_nf.tipo_utilizacao := v_bk_class_fis(TO_CHAR(v_nf.mnfst_serie)||TO_CHAR(v_nf.mdoc_cod)).tipo_utilizacao;
						EXCEPTION
						WHEN OTHERS THEN
							v_nf.tipo_utilizacao := NULL;
						END;
										
						${ATRIBUICAO_NF}
						
						-- cliente
						IF (v_cli.cli IS NULL OR v_cli.cli != v_bk_sanea(i).cli) THEN
						
						-- Atribuicao Cli
						v_cli.insere_reg          := 0;
						v_cli.insere_reg_comp     := 0;						  
						v_cli.update_reg          := 0;
						v_cli.update_reg_comp     := 0;
						v_cli.sit_reg             := 0;					  
						v_cli.cli                 := v_bk_sanea(i).cli;	
						v_cli.rowid_comp          := v_bk_sanea(i).rowid_comp             ;
						v_cli.cadg_num_conta      := v_bk_sanea(i).cadg_num_conta         ;
						v_cli.cadg_num_conta_new  := v_bk_sanea(i).cadg_num_conta         ;
						v_cli.cadg_tip_assin      := v_bk_sanea(i).cadg_tip_assin         ;
						v_cli.cadg_tip_cli        := v_bk_sanea(i).cadg_tip_cli           ;
						v_cli.cadg_dat_atua_comp  := v_bk_sanea(i).cadg_dat_atua_comp     ;	
						v_cli.cadg_dat_atua_new_comp  := v_bk_sanea(i).cadg_dat_atua_comp     ;						  
						v_cli.cadg_cod_new_comp   := v_bk_sanea(i).cadg_cod_comp          ;
						v_cli.cadg_cod_comp       := v_bk_sanea(i).cadg_cod_comp          ;
						v_cli.catg_cod_comp       := v_bk_sanea(i).catg_cod_comp          ;					  
						v_cli.cadg_uf_habilit     := v_bk_sanea(i).cadg_uf_habilit        ;
						v_cli.cadg_grp_tensao     := v_bk_sanea(i).cadg_grp_tensao        ;
						v_cli.cadg_tip_utiliz     := v_bk_sanea(i).cadg_tip_utiliz        ;
						v_cli.cadg_tel_contato    := v_bk_sanea(i).cadg_tel_contato       ;
						v_cli.rowid_cli           := v_bk_sanea(i).rowid_cli              ;
						v_cli.cadg_cod_cli        := v_bk_sanea(i).cadg_cod_cli           ;
						v_cli.cadg_cod_new_cli    := v_bk_sanea(i).cadg_cod_cli           ;
						v_cli.catg_cod_cli        := v_bk_sanea(i).catg_cod_cli           ;
						v_cli.cadg_dat_atua_new   := v_bk_sanea(i).cadg_dat_atua          ;
						v_cli.cadg_dat_atua       := v_bk_sanea(i).cadg_dat_atua          ;
						v_cli.cadg_cod_cgccpf     := v_bk_sanea(i).cadg_cod_cgccpf        ;
						v_cli.cadg_cod_insest     := v_bk_sanea(i).cadg_cod_insest        ;
						-- v_cli.cadg_tel            := v_bk_sanea(i).cadg_tel               ;
						-- v_cli.cadg_ddd_tel        := v_bk_sanea(i).cadg_ddd_tel           ;
						-- v_cli.pais_cod_cli        := v_bk_sanea(i).pais_cod_cli           ;
						-- v_cli.loca_cod_cli        := v_bk_sanea(i).loca_cod_cli           ;
						v_cli.unfe_sig_cli        := v_bk_sanea(i).unfe_sig_cli           ;
						v_cli.mibge_cod_mun_cli   := v_bk_sanea(i).mibge_cod_mun_cli      ;
						v_cli.cadg_end_munic_cli  := v_bk_sanea(i).cadg_end_munic_cli     ;
						v_cli.cadg_end_cep        := v_bk_sanea(i).cadg_end_cep           ;
						v_cli.cadg_end_bairro     := v_bk_sanea(i).cadg_end_bairro        ;
						-- v_cli.cadg_end_comp       := v_bk_sanea(i).cadg_end_comp          ;
						-- v_cli.cadg_end_num        := v_bk_sanea(i).cadg_end_num           ;
						-- v_cli.cadg_end            := v_bk_sanea(i).cadg_end               ;
						-- v_cli.cadg_nom_fantasia   := v_bk_sanea(i).cadg_nom_fantasia      ;
						-- v_cli.cadg_nom            := v_bk_sanea(i).cadg_nom               ;
						v_cli.cadg_tip            := v_bk_sanea(i).cadg_tip               ;
						-- v_cli.tp_loc_cli          := v_bk_sanea(i).tp_loc_cli             ;
						v_cli.var05_cli 			:= v_bk_sanea(i).var05_cli			    ;
						v_cli.var05_cli_new 	    := v_bk_sanea(i).var05_cli			    ;
						v_cli.var05_comp 			:= v_bk_sanea(i).var05_comp	 	        ; 	
						${ATRIBUICAO_CLI}
						
						END IF;						
		
					ELSE
					
						IF v_inf.infst_num_seq_max > v_bk_sanea(i).infst_num_seq_max THEN
							v_bk_sanea(i).infst_num_seq_max := v_inf.infst_num_seq_max;
						ELSIF v_inf.infst_num_seq_max < v_bk_sanea(i).infst_num_seq_max THEN
							v_inf.infst_num_seq_max := v_bk_sanea(i).infst_num_seq_max;
						END IF;				
						v_inf_aux.infst_num_seq_max        := v_inf.infst_num_seq_max;
						
						IF v_nf.mnfst_ind_cont_aux > v_bk_sanea(i).mnfst_ind_cont_aux THEN
							v_bk_sanea(i).mnfst_ind_cont_aux := v_nf.mnfst_ind_cont_aux;
						ELSIF v_nf.mnfst_ind_cont_aux < v_bk_sanea(i).mnfst_ind_cont_aux THEN
							v_nf.mnfst_ind_cont_aux := v_bk_sanea(i).mnfst_ind_cont_aux;
						END IF;							
					
					END IF;
					
					v_ds_etapa := substr('tratar inf: ' || v_inf.rowid_inf || ' >> ' || v_ds_etapa,1,4000);
					prcts_tratar_inf(p_cli1 => v_cli,p_f1 => v_f,	 p_inf1  => v_inf, p_nf1   => v_nf, p_sanea1 => v_bk_sanea(i), p_cp1  => v_cp, p_st_t1       => v_st_t,	 p_nr_qtde_inf1=> v_nr_qtde_inf);
												
					IF v_bk_sanea(i).last_reg_nf = 'S' THEN	
					
						v_ds_etapa := substr('tratar nf: ' || v_nf.rowid_nf || ' >> ' || v_ds_etapa,1,4000);	
						prcts_tratar_nf(p_cli1        => v_cli,   p_f1          => v_f,		    p_inf1        => v_inf,    p_nf1         => v_nf,   p_sanea1      => v_bk_sanea(i),  p_cp1         => v_cp);			
						v_nr_qtde_inf    := 0;
					
					END IF; 
					
					IF v_bk_sanea(i).last_reg_cli = 'S' THEN
					
						v_ds_etapa := substr('tratar cli: ' || v_cli.rowid_cli || ' >> ' || v_ds_etapa,1,4000);
						prcts_tratar_cli(p_cli1       => v_cli,	p_f1          => v_f,p_inf1        => v_inf,		p_nf1         => v_nf,	p_sanea1      => v_bk_sanea(i),		p_cp1         => v_cp);	
					
					END IF;	  
						
				END LOOP;    
		
				IF UPPER(TRIM('${COMMIT_SCRIPT}')) = 'COMMIT' THEN
				
					IF v_bk_nf.COUNT > 0 THEN
					
						v_ds_etapa := substr('update nf'|| ' >> ' || v_ds_etapa,1,4000);
						FORALL i IN v_bk_nf.FIRST .. v_bk_nf.LAST 
							UPDATE openrisow.mestre_nftl_serv ${PARTITION_NF} nf  
							SET -- nf.tdoc_cod                       = v_bk_nf(i).tdoc_cod             ,        
								nf.fili_cod                       = v_bk_nf(i).fili_cod             ,  
								nf.catg_cod                       = v_bk_nf(i).catg_cod             ,         
								nf.cadg_cod                       = v_bk_nf(i).cadg_cod             ,         
								nf.mnfst_ind_cont                 = v_bk_nf(i).mnfst_ind_cont       ,         
								nf.mnfst_val_tot                  = v_bk_nf(i).mnfst_val_tot        ,         
								nf.mnfst_val_desc                 = v_bk_nf(i).mnfst_val_desc       ,         
								nf.mnfst_ind_canc                 = v_bk_nf(i).mnfst_ind_canc       ,         
								-- nf.mnfst_dat_venc                 = v_bk_nf(i).mnfst_dat_venc       ,         
								nf.mnfst_per_ref                  = v_bk_nf(i).mnfst_per_ref        ,         
								-- nf.mnfst_avista                   = v_bk_nf(i).mnfst_avista         ,         
								-- nf.num01                          = v_bk_nf(i).num01                ,         
								-- nf.num02                          = v_bk_nf(i).num02                ,         
								-- nf.num03                          = v_bk_nf(i).num03                ,         
								-- nf.var01                          = v_bk_nf(i).var01                ,         
								-- nf.var02                          = v_bk_nf(i).var02                ,         
								-- nf.var03                          = v_bk_nf(i).var03                ,         
								-- nf.var04                          = v_bk_nf(i).var04                ,         
								nf.var05                          = v_bk_nf(i).var05                ,         
								-- nf.mnfst_ind_cnv115               = v_bk_nf(i).mnfst_ind_cnv115     ,         
								nf.cnpj_cpf                       = v_bk_nf(i).cnpj_cpf             ,         
								nf.mnfst_val_basicms              = v_bk_nf(i).mnfst_val_basicms    ,         
								nf.mnfst_val_icms                 = v_bk_nf(i).mnfst_val_icms       ,         
								nf.mnfst_val_isentas              = v_bk_nf(i).mnfst_val_isentas    ,         
								nf.mnfst_val_outras               = v_bk_nf(i).mnfst_val_outras     ,         
								nf.mnfst_codh_nf                  = v_bk_nf(i).mnfst_codh_nf        ,         
								-- nf.mnfst_codh_regnf               = v_bk_nf(i).mnfst_codh_regnf     ,         
								-- nf.mnfst_codh_regcli              = v_bk_nf(i).mnfst_codh_regcli    ,         
								-- nf.mnfst_reg_esp                  = v_bk_nf(i).mnfst_reg_esp        ,         
								-- nf.mnfst_bas_icms_st              = v_bk_nf(i).mnfst_bas_icms_st    ,         
								-- nf.mnfst_val_icms_st              = v_bk_nf(i).mnfst_val_icms_st    ,         
								-- nf.mnfst_val_pis                  = v_bk_nf(i).mnfst_val_pis        ,         
								-- nf.mnfst_val_cofins               = v_bk_nf(i).mnfst_val_cofins     ,         
								-- nf.mnfst_val_da                   = v_bk_nf(i).mnfst_val_da         ,         
								nf.mnfst_val_ser                  = v_bk_nf(i).mnfst_val_ser        ,         
								-- nf.mnfst_val_terc                 = v_bk_nf(i).mnfst_val_terc       ,         
								-- nf.cicd_cod_inf                   = v_bk_nf(i).cicd_cod_inf         ,         
								-- nf.mnfst_tip_assi                 = v_bk_nf(i).mnfst_tip_assi       ,         
								nf.mnfst_tip_util                 = v_bk_nf(i).mnfst_tip_util       ,         
								-- nf.mnfst_grp_tens                 = v_bk_nf(i).mnfst_grp_tens       ,         
								-- nf.mnfst_ind_extemp               = v_bk_nf(i).mnfst_ind_extemp     ,         
								-- nf.mnfst_dat_extemp               = v_bk_nf(i).mnfst_dat_extemp     ,         
								-- nf.mnfst_num_fic                  = v_bk_nf(i).mnfst_num_fic        ,         
								-- nf.mnfst_dt_lt_ant                = v_bk_nf(i).mnfst_dt_lt_ant      ,         
								-- nf.mnfst_dt_lt_atu                = v_bk_nf(i).mnfst_dt_lt_atu      , 
								-- nf.mnfst_vl_tot_fat               = v_bk_nf(i).mnfst_vl_tot_fat     ,         
								-- nf.mnfst_chv_nfe                  = v_bk_nf(i).mnfst_chv_nfe        ,         
								-- nf.mnfst_dat_aut_nfe              = v_bk_nf(i).mnfst_dat_aut_nfe    ,         
								-- nf.mnfst_val_desc_pis             = v_bk_nf(i).mnfst_val_desc_pis   ,         
								-- nf.mnfst_val_desc_cofins          = v_bk_nf(i).mnfst_val_desc_cofins,
								nf.mnfst_num_fat                  = v_bk_nf(i).mnfst_num_fat        
							WHERE nf.rowid = v_bk_nf(i).rowid_nf;
						
						v_bk_nf.delete;
					
					END IF;
				
				ELSE
				
					v_bk_nf.delete;
				
				END IF;
				
				IF UPPER(TRIM('${COMMIT_SCRIPT}')) = 'COMMIT' THEN
					
					IF v_bk_inf.COUNT > 0 THEN
					
						v_ds_etapa := substr('update inf'|| ' >> ' || v_ds_etapa,1,4000); 
						FORALL i IN v_bk_inf.FIRST .. v_bk_inf.LAST 
							UPDATE openrisow.item_nftl_serv ${PARTITION_INF} inf  
							SET inf.fili_cod                   = v_bk_inf(i).fili_cod             ,
								inf.cgc_cpf                    = v_bk_inf(i).cgc_cpf              ,
								inf.ie                         = v_bk_inf(i).ie                   ,
								inf.uf                         = v_bk_inf(i).uf                   ,
								-- inf.tp_loc                     = v_bk_inf(i).tp_loc               ,
								-- inf.localidade                 = v_bk_inf(i).localidade           ,
								-- inf.tdoc_cod                   = v_bk_inf(i).tdoc_cod             ,
								inf.catg_cod                   = v_bk_inf(i).catg_cod             ,
								inf.cadg_cod                   = v_bk_inf(i).cadg_cod             ,
								inf.serv_cod                   = v_bk_inf(i).serv_cod             ,
								inf.estb_cod                   = v_bk_inf(i).estb_cod             ,
								-- inf.infst_dsc_compl            = v_bk_inf(i).infst_dsc_compl      ,
								inf.infst_val_cont             = v_bk_inf(i).infst_val_cont       ,
								inf.infst_val_serv             = v_bk_inf(i).infst_val_serv       ,
								inf.infst_val_desc             = v_bk_inf(i).infst_val_desc       ,
								inf.infst_aliq_icms            = v_bk_inf(i).infst_aliq_icms      ,
								inf.infst_base_icms            = v_bk_inf(i).infst_base_icms      ,
								inf.infst_val_icms             = v_bk_inf(i).infst_val_icms       ,
								inf.infst_isenta_icms          = v_bk_inf(i).infst_isenta_icms    ,
								inf.infst_outras_icms          = v_bk_inf(i).infst_outras_icms    ,
								-- inf.infst_tribipi              = v_bk_inf(i).infst_tribipi        ,
								inf.infst_tribicms             = v_bk_inf(i).infst_tribicms       ,
								-- inf.infst_isenta_ipi           = v_bk_inf(i).infst_isenta_ipi     ,
								-- inf.infst_outra_ipi            = v_bk_inf(i).infst_outra_ipi      ,
								-- inf.infst_outras_desp          = v_bk_inf(i).infst_outras_desp    ,
								-- inf.infst_fiscal               = v_bk_inf(i).infst_fiscal         ,
								inf.infst_num_seq              = v_bk_inf(i).infst_num_seq        ,
								inf.infst_tel                  = v_bk_inf(i).infst_tel            ,
								inf.infst_ind_canc             = v_bk_inf(i).infst_ind_canc       ,
								-- inf.infst_proter               = v_bk_inf(i).infst_proter         ,
								-- inf.infst_cod_cont             = v_bk_inf(i).infst_cod_cont       ,
								inf.cfop                       = v_bk_inf(i).cfop                 ,
								inf.mdoc_cod                   = v_bk_inf(i).mdoc_cod             ,
								-- inf.cod_prest                  = v_bk_inf(i).cod_prest            ,
								-- inf.num01                      = v_bk_inf(i).num01                ,
								-- inf.num02                      = v_bk_inf(i).num02                ,
								-- inf.num03                      = v_bk_inf(i).num03                ,
								-- inf.var01                      = v_bk_inf(i).var01                ,
								-- inf.var02                      = v_bk_inf(i).var02                ,
								-- inf.var03                      = v_bk_inf(i).var03                ,
								-- inf.var04                      = v_bk_inf(i).var04                ,
								-- inf.infst_ind_cnv115           = v_bk_inf(i).infst_ind_cnv115     ,
								-- inf.infst_unid_medida          = v_bk_inf(i).infst_unid_medida    ,
								-- inf.infst_quant_contr          = v_bk_inf(i).infst_quant_contr    ,
								-- inf.infst_quant_prest          = v_bk_inf(i).infst_quant_prest    ,
								-- inf.infst_codh_reg             = v_bk_inf(i).infst_codh_reg       ,
								inf.esta_cod                   = v_bk_inf(i).esta_cod             ,
								inf.infst_val_pis              = v_bk_inf(i).infst_val_pis        ,
								inf.infst_val_cofins           = v_bk_inf(i).infst_val_cofins     ,
								-- inf.infst_bas_icms_st          = v_bk_inf(i).infst_bas_icms_st    ,
								-- inf.infst_aliq_icms_st         = v_bk_inf(i).infst_aliq_icms_st   ,
								-- inf.infst_val_icms_st          = v_bk_inf(i).infst_val_icms_st    ,
								inf.infst_val_red              = v_bk_inf(i).infst_val_red        ,
								-- inf.tpis_cod                   = v_bk_inf(i).tpis_cod             ,
								-- inf.tcof_cod                   = v_bk_inf(i).tcof_cod             ,
								-- inf.infst_bas_piscof           = v_bk_inf(i).infst_bas_piscof     ,
								-- inf.infst_aliq_pis             = v_bk_inf(i).infst_aliq_pis       ,
								-- inf.infst_aliq_cofins          = v_bk_inf(i).infst_aliq_cofins    ,
								-- inf.infst_nat_rec              = v_bk_inf(i).infst_nat_rec        ,
								-- inf.cscp_cod                   = v_bk_inf(i).cscp_cod             ,
								inf.infst_num_contr            = v_bk_inf(i).infst_num_contr      ,
								-- inf.infst_tip_isencao          = v_bk_inf(i).infst_tip_isencao    ,
								-- inf.infst_tar_aplic            = v_bk_inf(i).infst_tar_aplic      ,
								-- inf.infst_ind_desc             = v_bk_inf(i).infst_ind_desc       ,
								-- inf.infst_num_fat              = v_bk_inf(i).infst_num_fat        ,
								-- inf.infst_qtd_fat              = v_bk_inf(i).infst_qtd_fat        ,
								-- inf.infst_mod_ativ             = v_bk_inf(i).infst_mod_ativ       ,
								-- inf.infst_hora_ativ            = v_bk_inf(i).infst_hora_ativ      ,
								-- inf.infst_id_equip             = v_bk_inf(i).infst_id_equip       ,
								-- inf.infst_mod_pgto             = v_bk_inf(i).infst_mod_pgto       ,
								-- inf.infst_num_nfe              = v_bk_inf(i).infst_num_nfe        ,
								-- inf.infst_dtemiss_nfe          = v_bk_inf(i).infst_dtemiss_nfe    ,
								-- inf.infst_val_cred_nfe         = v_bk_inf(i).infst_val_cred_nfe   ,
								-- inf.infst_cnpj_can_com         = v_bk_inf(i).infst_cnpj_can_com   ,
								-- inf.infst_val_desc_pis         = v_bk_inf(i).infst_val_desc_pis   ,
								-- inf.infst_val_desc_cofins      = v_bk_inf(i).infst_val_desc_cofins,
								inf.var05                      = v_bk_inf(i).var05 
							WHERE inf.rowid = v_bk_inf(i).rowid_inf;				
						
						v_bk_inf.delete;
						
					END IF;
					
				ELSE
				
					v_bk_inf.delete;
				
				END IF;
				
				
				IF UPPER(TRIM('${COMMIT_SCRIPT}')) = 'COMMIT' THEN
		
					IF v_bk_cli.COUNT > 0 THEN
		
						v_ds_etapa := substr('update cli'|| ' >> ' || v_ds_etapa,1,4000);
						FORALL i IN v_bk_cli.FIRST .. v_bk_cli.LAST 
							UPDATE openrisow.cli_fornec_transp cli 
							SET cli.catg_cod                       = v_bk_cli(i).catg_cod_cli             ,         
								-- cli.cadg_cod                       = v_bk_cli(i).cadg_cod_cli             ,         
								cli.cadg_dat_atua 				   = v_bk_cli(i).cadg_dat_atua            ,   
								cli.cadg_cod_cgccpf                = v_bk_cli(i).cadg_cod_cgccpf          ,
								cli.cadg_cod_insest                = v_bk_cli(i).cadg_cod_insest          ,
								-- cli.cadg_tel                       = v_bk_cli(i).cadg_tel                 ,
								-- cli.cadg_ddd_tel                   = v_bk_cli(i).cadg_ddd_tel             ,
								-- cli.pais_cod 					   = v_bk_cli(i).pais_cod_cli             ,
								-- cli.loca_cod 					   = v_bk_cli(i).loca_cod_cli             ,
								cli.unfe_sig   					   = v_bk_cli(i).unfe_sig_cli             ,
								cli.mibge_cod_mun                  = v_bk_cli(i).mibge_cod_mun_cli        ,
								cli.cadg_end_munic                 = v_bk_cli(i).cadg_end_munic_cli       ,
								cli.cadg_end_cep                   = v_bk_cli(i).cadg_end_cep             ,
								cli.cadg_end_bairro                = v_bk_cli(i).cadg_end_bairro          ,
								-- cli.cadg_end_comp                  = v_bk_cli(i).cadg_end_comp            ,
								-- cli.cadg_end_num                   = v_bk_cli(i).cadg_end_num             ,
								-- cli.cadg_end                       = v_bk_cli(i).cadg_end                 ,
								-- cli.cadg_nom_fantasia              = v_bk_cli(i).cadg_nom_fantasia        ,
								-- cli.cadg_nom                       = v_bk_cli(i).cadg_nom                 ,
								cli.cadg_tip                       = v_bk_cli(i).cadg_tip                 ,
								-- cli.tp_loc                         = v_bk_cli(i).tp_loc_cli               ,
								cli.var05                          = v_bk_cli(i).var05_cli						
							WHERE cli.rowid = v_bk_cli(i).rowid_cli;				
		
						v_bk_cli.delete;
		
					END IF;
				
				ELSE
				
					v_bk_cli.delete;
				
				END IF;
				
				IF UPPER(TRIM('${COMMIT_SCRIPT}')) = 'COMMIT' THEN
					
					IF v_bk_comp.COUNT > 0 THEN
						
						v_ds_etapa := substr('update comp'|| ' >> ' || v_ds_etapa,1,4000);
						FORALL i IN v_bk_comp.FIRST .. v_bk_comp.LAST 
							UPDATE openrisow.complvu_clifornec comp 
							SET   comp.cadg_num_conta           = v_bk_comp(i).cadg_num_conta               ,
								comp.cadg_tip_assin           = v_bk_comp(i).cadg_tip_assin               ,
								comp.cadg_tip_cli             = v_bk_comp(i).cadg_tip_cli                 ,
								comp.cadg_dat_atua            = v_bk_comp(i).cadg_dat_atua_comp           ,						  
								comp.cadg_cod                 = v_bk_comp(i).cadg_cod_comp                ,
								comp.catg_cod                 = v_bk_comp(i).catg_cod_comp                ,
								comp.cadg_uf_habilit          = v_bk_comp(i).cadg_uf_habilit              ,
								comp.cadg_grp_tensao          = v_bk_comp(i).cadg_grp_tensao              , 
								comp.cadg_tip_utiliz          = v_bk_comp(i).cadg_tip_utiliz              
								,	  comp.cadg_tel_contato         = v_bk_comp(i).cadg_tel_contato 
								,      comp.var05                    = v_bk_comp(i).var05_comp		
							WHERE comp.rowid = v_bk_comp(i).rowid_comp;				
						v_bk_comp.delete;
					
					END IF;
				
				ELSE
				
					v_bk_comp.delete;
				
				END IF;
				
				IF UPPER(TRIM('${COMMIT_SCRIPT}')) = 'COMMIT' THEN
		
					BEGIN
						v_error_bk := NULL;
						IF v_bk_cli_ins.COUNT > 0 THEN
							v_ds_etapa := substr('insere cli'|| ' >> ' || v_ds_etapa,1,4000);
							FORALL i IN v_bk_cli_ins.FIRST .. v_bk_cli_ins.LAST  SAVE EXCEPTIONS
								INSERT INTO openrisow.cli_fornec_transp
								(CADG_COD          ,
									CADG_DAT_ATUA     ,
									CATG_COD          ,
									PAIS_COD          ,
									UNFE_SIG          ,
									CADG_COD_CGCCPF   ,
									CADG_TIP          ,
									CADG_COD_INSEST   ,
									CADG_COD_INSMUN   ,
									EQUIPAR_RURAL     ,
									CADG_NOM          ,
									CADG_NOM_FANTASIA ,
									CADG_END          ,
									CADG_END_NUM      ,
									CADG_END_COMP     ,
									CADG_END_BAIRRO   ,
									CADG_END_MUNIC    ,
									CADG_END_CEP      ,
									CADG_IND_COLIGADA ,
									CADG_COD_SUFRAMA  ,
									TP_LOC            ,
									LOCA_COD          ,
									CADG_CEI          ,
									NUM01             ,
									NUM02             ,
									NUM03             ,
									VAR01             ,
									VAR02             ,
									VAR03             ,
									VAR04             ,
									VAR05             ,
									CADG_NIT          ,
									CADG_CX_POST      ,
									CADG_CEP_CXP      ,
									CADG_DDD_TEL      ,
									CADG_TEL          ,
									CADG_DDD_FAX      ,
									CADG_FAX          ,
									CADG_CLAS_RI      ,
									MIBGE_COD_MUN     ,
									CADG_DAT_LAUDO    ,
									CADG_IND_NIF      ,
									CADG_DSC_NIF      ,
									IREX_COD          ,
									CADG_IND_OB_CIVIL ,
									SIST_ORIGEM       ,
									USUA_ORIGEM       ,
									DATA_CRIACAO      ,
									ID_ORIGEM         ,
									FTRB_COD          ,
									CADG_INF_ISEN     ,	
									CADG_PROVINCIA    ) -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
								SELECT 
									v_bk_cli_ins(i).cadg_cod_new_cli   AS CADG_COD,          
									v_bk_cli_ins(i).cadg_dat_atua_new  AS CADG_DAT_ATUA,
									CATG_COD          ,
									PAIS_COD          ,
									UNFE_SIG          ,
									CADG_COD_CGCCPF   ,
									CADG_TIP          ,
									CADG_COD_INSEST   ,
									CADG_COD_INSMUN   ,
									EQUIPAR_RURAL     ,
									CADG_NOM          ,
									CADG_NOM_FANTASIA ,
									CADG_END          ,
									CADG_END_NUM      ,
									CADG_END_COMP     ,
									CADG_END_BAIRRO   ,
									CADG_END_MUNIC    ,
									CADG_END_CEP      ,
									CADG_IND_COLIGADA ,
									CADG_COD_SUFRAMA  ,
									TP_LOC            ,
									LOCA_COD          ,
									CADG_CEI          ,
									NUM01             ,
									NUM02             ,
									NUM03             ,
									VAR01             ,
									VAR02             ,
									VAR03             ,
									VAR04             ,
									v_bk_cli_ins(i).var05_cli_new AS VAR05             ,
									CADG_NIT          ,
									CADG_CX_POST      ,
									CADG_CEP_CXP      ,
									CADG_DDD_TEL      ,
									CADG_TEL          ,
									CADG_DDD_FAX      ,
									CADG_FAX          ,
									CADG_CLAS_RI      ,
									MIBGE_COD_MUN     ,
									CADG_DAT_LAUDO    ,
									CADG_IND_NIF      ,
									CADG_DSC_NIF      ,
									IREX_COD          ,
									CADG_IND_OB_CIVIL ,
									SIST_ORIGEM       ,
									USUA_ORIGEM       ,
									DATA_CRIACAO      ,
									ID_ORIGEM         ,
									FTRB_COD          ,
									CADG_INF_ISEN     ,	
									CADG_PROVINCIA     -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
								FROM  openrisow.cli_fornec_transp cli      		
								WHERE cli.rowid = v_bk_cli_ins(i).rowid_cli;				
							v_bk_cli_ins.delete;				
						END IF;		
		
					EXCEPTION
					WHEN ex_dml_errors THEN
						BEGIN 
							l_error_count := SQL%BULK_EXCEPTIONS.count;
							FOR i IN 1 .. l_error_count LOOP			
									IF -SQL%BULK_EXCEPTIONS(i).ERROR_CODE != -1 THEN
										v_error_bk    := SUBSTR('Error: ' || i ||  
																' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||  
																' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),1,500)
														|| ' | ' ||
														SUBSTR(v_error_bk,1,3490);		
									END IF;				 
							END LOOP;
						EXCEPTION
								WHEN OTHERS THEN
										v_error_bk    := NULL;
						END;
						IF NVL(LENGTH(TRIM(v_error_bk)),0) > 0 THEN
							v_error_bk    := SUBSTR('Number of failures: ' || l_error_count,1,500)
												|| ' | ' ||
												SUBSTR(v_error_bk,1,3490);
							prcts_stop(v_error_bk);					 
						END IF;
					END;		
				
				ELSE
				
					v_bk_cli_ins.delete;
				
				END IF;
				
				IF UPPER(TRIM('${COMMIT_SCRIPT}')) = 'COMMIT' THEN
		
					BEGIN
		
						v_error_bk := NULL;
						IF v_bk_comp_ins.COUNT > 0 THEN
							v_ds_etapa := substr('insere comp'|| ' >> ' || v_ds_etapa,1,4000);
							FORALL i IN v_bk_comp_ins.FIRST .. v_bk_comp_ins.LAST  SAVE EXCEPTIONS
								INSERT INTO openrisow.complvu_clifornec
									(   CADG_COD         ,
										CATG_COD         ,
										CADG_DAT_ATUA    ,
										CADG_TIP_ASSIN   ,
										CADG_TIP_UTILIZ  ,
										CADG_GRP_TENSAO  ,
										CADG_TEL_CONTATO ,
										CADG_NUM_CONTA   ,
										CADG_UF_HABILIT  ,
										CADG_TIP_CLI     ,
										CADG_SUB_CONSU   ,	
										NUM01  			 ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										NUM02 			 ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										NUM03            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										VAR01            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	
										VAR02            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
										VAR03      		 ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185		
										VAR04            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185	
										VAR05            )  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
								SELECT 	v_bk_comp_ins(i).cadg_cod_new_comp AS CADG_COD,
										CATG_COD         ,
										v_bk_comp_ins(i).cadg_dat_atua_new_comp AS CADG_DAT_ATUA    ,
										CADG_TIP_ASSIN   ,
										CADG_TIP_UTILIZ  ,
										CADG_GRP_TENSAO  ,
										CADG_TEL_CONTATO ,
										v_bk_comp_ins(i).cadg_num_conta_new AS CADG_NUM_CONTA   ,
										CADG_UF_HABILIT  ,
										CADG_TIP_CLI     ,
										CADG_SUB_CONSU   ,	
										NUM01            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										NUM02            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										NUM03            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										VAR01            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										VAR02            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										VAR03            ,	-- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
										VAR04            ,  -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185 
										VAR05               -- ++ ADEQUACAO_MAPA_PARA_GF_PATH_185
								FROM openrisow.complvu_clifornec comp 			     		
								WHERE comp.rowid = v_bk_comp_ins(i).rowid_comp;				
							v_bk_comp_ins.delete;				
						END IF;		
		
					EXCEPTION
					WHEN ex_dml_errors THEN			   
		
						BEGIN 
							l_error_count := SQL%BULK_EXCEPTIONS.count;
							FOR i IN 1 .. l_error_count LOOP			
									IF -SQL%BULK_EXCEPTIONS(i).ERROR_CODE != -1 THEN
										v_error_bk    := SUBSTR('Error: ' || i ||  
																' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||  
																' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),1,500)
														|| ' | ' ||
														SUBSTR(v_error_bk,1,3490);		
									END IF;				 
							END LOOP;
						EXCEPTION
								WHEN OTHERS THEN
										v_error_bk    := NULL;
						END;
						IF NVL(LENGTH(TRIM(v_error_bk)),0) > 0 THEN
							v_error_bk    := SUBSTR('Number of failures: ' || l_error_count,1,500)
												|| ' | ' ||
												SUBSTR(v_error_bk,1,3490);
							prcts_stop(v_error_bk);					 
						END IF;
						
					END;			
				
				ELSE
				
					v_bk_comp_ins.delete;
					
				END IF;
				
				v_bk_st_t3.delete;
				IF UPPER(TRIM('${COMMIT_SCRIPT}')) = 'COMMIT' THEN
				
					BEGIN
						v_nro_idx    := 0;
						v_idx        := v_bk_st_t.first;
						WHILE (v_idx IS NOT NULL)
						LOOP
						IF v_bk_st_t.exists(v_idx) THEN
							BEGIN
							
							
							INSERT
							INTO openrisow.servico_telcom
								(
								emps_cod,
								fili_cod,
								servtl_dat_atua,
								servtl_cod,
								clasfi_cod,
								servtl_desc,
								servtl_compl,
								servtl_ind_tprec,
								servtl_ind_tpserv,
								servtl_cod_nat,
								var01,
								var02,
								var03,
								var04,
								var05,
								num01,
								num02,
								num03,
								servtl_ind_rec,
								servtl_tip_utiliz
								)
								VALUES
								(
								v_bk_st_t(v_idx).emps_cod,
								v_bk_st_t(v_idx).fili_cod,
								v_bk_st_t(v_idx).servtl_dat_atua,
								v_bk_st_t(v_idx).servtl_cod,
								v_bk_st_t(v_idx).clasfi_cod,
								v_bk_st_t(v_idx).servtl_desc,
								v_bk_st_t(v_idx).servtl_compl,
								v_bk_st_t(v_idx).servtl_ind_tprec,
								v_bk_st_t(v_idx).servtl_ind_tpserv,
								v_bk_st_t(v_idx).servtl_cod_nat,
								v_bk_st_t(v_idx).var01,
								v_bk_st_t(v_idx).var02,
								v_bk_st_t(v_idx).var03,
								v_bk_st_t(v_idx).var04,
								v_bk_st_t(v_idx).var05,
								v_bk_st_t(v_idx).num01,
								v_bk_st_t(v_idx).num02,
								v_bk_st_t(v_idx).num03,
								v_bk_st_t(v_idx).servtl_ind_rec,
								v_bk_st_t(v_idx).servtl_tip_utiliz
								);
							EXCEPTION
							WHEN DUP_VAL_ON_INDEX THEN
							UPDATE openrisow.servico_telcom
							SET var05           = v_bk_st_t(v_idx).var05 ,
								clasfi_cod        = v_bk_st_t(v_idx).clasfi_cod ,
								SERVTL_TIP_UTILIZ = v_bk_st_t(v_idx).SERVTL_TIP_UTILIZ
							WHERE EMPS_COD      = v_bk_st_t(v_idx).EMPS_COD
							AND FILI_COD        = v_bk_st_t(v_idx).FILI_COD
							AND SERVTL_DAT_ATUA = v_bk_st_t(v_idx).SERVTL_DAT_ATUA
							AND SERVTL_COD      = v_bk_st_t(v_idx).SERVTL_COD;
							END;
							v_nro_idx := v_nro_idx + 1;
						END IF;
						v_idx := v_bk_st_t.next(v_idx);
						END LOOP;
						IF v_nro_idx > 0 THEN
						v_bk_st_t.delete;
						END IF;
					END;			
				END IF;
			
			ELSE
			
				v_bk_st_t.delete;
			
			END IF; 
			
			${COMMIT_SCRIPT};
			prcts_etapa('${COMMIT_SCRIPT};',TRUE);
			prcts_insere_process_log;
		
			EXIT WHEN c_sanea%NOTFOUND;	  
		
		END LOOP;        
		CLOSE c_sanea; 
		
		
		GOTO FIM;
		
		RETURN;
		
	END;         
	
	
	<<FIM>>		
	BEGIN
			
		${COMMIT_SCRIPT};
			
		prcts_etapa('Processados ${COMMIT_SCRIPT} >>  : ' || :v_qtd_processados || ' | NF : ' || :v_qtd_atu_nf || ' | INF : ' || :v_qtd_atu_inf|| ' | CLI : ' || :v_qtd_atu_cli);
		prcts_etapa('FIM',TRUE);	
		:v_msg_erro :=   substr(substr(nvl(v_ds_etapa,' '),1,3000) || ' <||> ' || substr(:v_msg_erro,1,990) ,1,4000);
			
		-----------------------------------------------------------------------------
		--> Eliminando
		-----------------------------------------------------------------------------
		DBMS_APPLICATION_INFO.set_module(null,null);
		DBMS_APPLICATION_INFO.set_client_info (null);
		
		RETURN;
		
	END ;
	
EXCEPTION           
WHEN OTHERS THEN 
	ROLLBACK;     
	prcts_etapa('ERRO : ' || SUBSTR(SQLERRM,1,500),TRUE);
	:v_msg_erro :=   substr(substr(nvl(v_ds_etapa,' '),1,3000) || ' <||> ' || substr(:v_msg_erro,1,990) ,1,4000);
	:v_st_processamento := 'Erro';
	:exit_code := 1;
	-----------------------------------------------------------------------------
	--> Eliminando
	-----------------------------------------------------------------------------
	DBMS_APPLICATION_INFO.set_module(null,null);
	DBMS_APPLICATION_INFO.set_client_info (null);	  
END;       
/                   
                    
PROMPT Processado   
ROLLBACK;           
UPDATE gfcadastro.CONTROLE_PROCESSAMENTO cp
   SET cp.dt_fim_proc            = SYSDATE,
       cp.st_processamento       = :v_st_processamento,
       cp.ds_msg_erro            = substr(substr(nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_atualizados_nf      = NVL(cp.qt_atualizados_nf,0)       + :v_qtd_atu_nf,
       cp.qt_atualizados_inf     = NVL(cp.qt_atualizados_inf,0)      + :v_qtd_atu_inf,
       cp.qt_atualizados_cli     = NVL(cp.qt_atualizados_cli,0)      + :v_qtd_atu_cli,
       cp.qt_atualizados_comp    = NVL(cp.qt_atualizados_comp,0)     + :v_qtd_atu_comp,
	   cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0)   + :v_qtd_atu_st
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;             
                    
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

