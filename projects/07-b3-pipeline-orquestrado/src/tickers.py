"""
Ativos da B3 validados no yfinance (dez/2024).
Divididos por tipo: ações do Ibovespa e FIIs mais negociados.
"""

STOCKS = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "B3SA3.SA",
    "ABEV3.SA", "WEGE3.SA", "RENT3.SA", "EQTL3.SA", "BBAS3.SA",
    "SUZB3.SA", "RADL3.SA", "GGBR4.SA", "RAIL3.SA", "VIVT3.SA",
    "CSNA3.SA", "CMIG4.SA", "TOTS3.SA", "HAPV3.SA", "PRIO3.SA",
    "ENEV3.SA", "SBSP3.SA", "BPAC11.SA", "LREN3.SA", "UGPA3.SA",
    "KLBN11.SA", "CSAN3.SA", "RDOR3.SA", "TAEE11.SA", "ENGI11.SA",
    "VBBR3.SA", "BRAP4.SA", "GOAU4.SA", "MULT3.SA", "BEEF3.SA",
    "COGN3.SA", "CYRE3.SA", "MRVE3.SA", "MGLU3.SA", "YDUQ3.SA",
    "IRBR3.SA", "CMIN3.SA", "ALPA4.SA", "LWSA3.SA", "SMTO3.SA",
    "SLCE3.SA", "RECV3.SA", "AURE3.SA",
]

FIIS = [
    "HGLG11.SA", "XPML11.SA", "MXRF11.SA", "KNRI11.SA", "VISC11.SA",
    "BTLG11.SA", "PVBI11.SA", "CPTS11.SA", "KNCR11.SA", "HGBS11.SA",
    "XPLG11.SA", "VILG11.SA", "HGRE11.SA", "RBRR11.SA", "IRDM11.SA",
]

ALL_TICKERS = STOCKS + FIIS

ASSET_TYPE = {t: "STOCK" for t in STOCKS} | {t: "FII" for t in FIIS}
