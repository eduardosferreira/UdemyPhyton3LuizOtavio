import unittest
from src.main import fnc_teste_01


class TesteSoma(unittest.TestCase):
    def test_retorno_soma_10_10_001(self):
        self.assertEqual(fnc_teste_01(10, 10), 20)

    def test_retorno_soma_10_10_002(self):
        self.assertEqual(fnc_teste_01(5, 10), 20)
