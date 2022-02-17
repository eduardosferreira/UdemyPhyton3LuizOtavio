#!/bin/bash
clear
############################ PARAMETROS DE ENTRADA ###################################################
export PROCESSO="${1:-NM_PRC_NOVO_MAPA_99999999}" 
export DATA_INICIO="${2:-03/01/2049}"
export DATA_FIM="${3:-04/01/2049}"
export FILTRO="${4:-1=1 AND nf.emps_cod= 'TBRA' AND nf.fili_cod = '0000'}" 
export BASE="${5:-Clone 21}" 
export COMMIT="${6:-ROLLBACK}" 
export REGRAS_HISTORIAS="${7:-NOVO_MAPA}"
export STATUS_PROCESSO="${8:-Erro,Aguardando,Reprocessar,Em Processamento}" # Erro,Aguardando,Reprocessar,Em Processamento
export TABELA_NF="${9:-MESTRE_NFTL_SERV}"
export TABELA_INF="${10:-ITEM_NFTL_SERV}"
export SCRIPT_REGRA_BEFORE_01="${11:-185/MAP_2_REGRA_ISOLADAS/MAP_2_REGRA_46}"
export FIND=" "
export REPLACE=""
############################ PARAMETROS DIVERSOS ###################################################
./controle_paralelismo.sh "${PROCESSO}" "${DATA_INICIO}" "${DATA_FIM}" "${FILTRO}" "${BASE}" "${COMMIT}" "${REGRAS_HISTORIAS}" "${STATUS_PROCESSO}" "${TABELA_NF}" "${TABELA_INF}" "${SCRIPT_REGRA_BEFORE_01}"
RETORNO=$?
exit ${RETORNO}
