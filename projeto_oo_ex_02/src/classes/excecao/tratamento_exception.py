# -*- coding: utf-8 -*-
"""Classe para tratamento do proprio erro
"""


class TratamentoException(Exception):
    """Tratamento do proprio controle de erro a ser gerado

    Args:
        Exception ([type]): [description]
    """

    def __init__(self, *args: object) -> None:
        """Construtor da super classe herdado de Excpetion
        """
        super().__init__(*args)
