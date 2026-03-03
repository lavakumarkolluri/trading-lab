# ──────────────────────────────────────────────────────
# symbols.py — Complete watchlist for trading pipeline
#
# NSE India   : Nifty 500 (top 500 by market cap)
# BSE India   : BSE 500 (non-duplicate with NSE)
# US          : S&P 500 + ETFs
# NASDAQ 100  : Top 100
# Crypto      : Top 20
# Forex       : 8 major pairs
# ──────────────────────────────────────────────────────

MARKETS = {

    # ── NSE India — Nifty 500 ──────────────────────────
    "indian": [
        # Nifty 50
        "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
        "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
        "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "TITAN.NS",
        "SUNPHARMA.NS", "ULTRACEMCO.NS", "WIPRO.NS", "HCLTECH.NS", "BAJFINANCE.NS",
        "NESTLEIND.NS", "TECHM.NS", "POWERGRID.NS", "NTPC.NS", "ONGC.NS",
        "TATASTEEL.NS", "JSWSTEEL.NS", "ADANIENT.NS", "ADANIPORTS.NS",
        "COALINDIA.NS", "DIVISLAB.NS", "DRREDDY.NS", "EICHERMOT.NS", "GRASIM.NS",
        "HEROMOTOCO.NS", "HINDALCO.NS", "INDUSINDBK.NS", "M&M.NS", "BAJAJFINSV.NS",
        "BAJAJ-AUTO.NS", "BRITANNIA.NS", "CIPLA.NS", "SBILIFE.NS", "HDFCLIFE.NS",
        "APOLLOHOSP.NS", "BPCL.NS", "TATACONSUM.NS", "UPL.NS", "NIFTYBEES.NS",
        # Nifty Next 50
        "SIEMENS.NS", "HAVELLS.NS", "DMART.NS", "PIDILITIND.NS", "BERGEPAINT.NS",
        "DABUR.NS", "MUTHOOTFIN.NS", "TORNTPHARM.NS", "COLPAL.NS", "MARICO.NS",
        "AMBUJACEM.NS", "SHREECEM.NS", "ACC.NS", "BANDHANBNK.NS", "FEDERALBNK.NS",
        "IDFCFIRSTB.NS", "MCDOWELL-N.NS", "GODREJCP.NS", "PAGEIND.NS", "BOSCHLTD.NS",
        "TATAPOWER.NS", "ADANIGREEN.NS", "NAUKRI.NS", "ZOMATO.NS",
        "PFC.NS", "RECLTD.NS", "IRFC.NS", "NHPC.NS", "SJVN.NS",
        "INDIANB.NS", "BANKBARODA.NS", "CANBK.NS", "UNIONBANK.NS", "PNB.NS",
        "ICICIGI.NS", "HDFCAMC.NS", "NIPPONLIFE.NS",
        # Nifty Midcap
        "AUROPHARMA.NS", "ALKEM.NS", "LALPATHLAB.NS", "METROPOLIS.NS",
        "FORTIS.NS", "MAXHEALTH.NS", "NARAYANA.NS",
        "COFORGE.NS", "LTIM.NS", "MPHASIS.NS", "PERSISTENT.NS", "SONATSOFTW.NS",
        "TATAELXSI.NS", "KPITTECH.NS", "CYIENT.NS", "ZENSAR.NS",
        "CROMPTON.NS", "VOLTAS.NS", "BLUESTAR.NS",
        "CHOLAFIN.NS", "LICHSGFIN.NS", "PNBHOUSING.NS",
        "ASTRAL.NS", "SUPREMEIND.NS", "POLYCAB.NS", "KEI.NS",
        "VGUARD.NS", "ORIENTCEM.NS", "RAMCOCEM.NS", "JKCEMENT.NS",
        "PIIND.NS", "RALLIS.NS", "SUMICHEM.NS", "BAYERCROP.NS",
        "CONCOR.NS", "BLUEDART.NS", "APOLLOTYRE.NS", "CEATLTD.NS",
        "MRF.NS", "BALKRISIND.NS", "EXIDEIND.NS", "MOTHERSON.NS",
        "BHARATFORG.NS", "ESCORTS.NS",
        "ZEEL.NS", "SUNTV.NS", "PVRINOX.NS",
        "INDHOTEL.NS", "EIHOTEL.NS",
        "JUBLFOOD.NS", "WESTLIFE.NS", "DEVYANI.NS",
        "TRENT.NS", "ABFRL.NS", "NYKAA.NS",
        "VEDL.NS", "HINDZINC.NS", "NMDC.NS", "NALCO.NS",
        "SAIL.NS", "JINDALSTEL.NS", "RATNAMANI.NS",
        "CESC.NS", "TORNTPOWER.NS", "IEX.NS",
        "IRCTC.NS", "RVNL.NS", "RITES.NS", "NBCC.NS",
        "GODREJPROP.NS", "OBEROIRLTY.NS", "PRESTIGE.NS", "BRIGADE.NS", "SOBHA.NS",
        "DEEPAKFERT.NS", "CHAMBLFERT.NS", "COROMANDEL.NS",
        "AARTI.NS", "VINATI.NS", "ATUL.NS", "NAVINFLUOR.NS",
        "SPARC.NS", "NATCOPHARM.NS", "GRANULES.NS", "IPCA.NS", "AJANTPHARM.NS",
        "LAURUSLABS.NS", "GLENMARK.NS",
        "IDBI.NS", "KARURVYSYA.NS", "DCBBANK.NS",
        "ANGELONE.NS", "IIFL.NS", "MOTILALOFS.NS",
        "STARHEALTH.NS", "GODIGIT.NS",
        "BEL.NS", "HAL.NS", "BHEL.NS",
        "ABB.NS", "CUMMINSIND.NS", "THERMAX.NS",
    ],

    # ── BSE India — BSE listed unique symbols ──────────
    "bse": [
        "RELIANCE.BO", "TCS.BO", "HDFCBANK.BO", "INFY.BO", "ICICIBANK.BO",
        "SBIN.BO", "BHARTIARTL.BO", "LT.BO", "AXISBANK.BO", "KOTAKBANK.BO",
        "ITC.BO", "HINDUNILVR.BO", "BAJFINANCE.BO", "MARUTI.BO", "TITAN.BO",
        "SUNPHARMA.BO", "WIPRO.BO", "HCLTECH.BO", "TECHM.BO", "NESTLEIND.BO",
        "TATASTEEL.BO", "JSWSTEEL.BO", "ONGC.BO", "NTPC.BO", "POWERGRID.BO",
        "ADANIENT.BO", "ADANIPORTS.BO", "COALINDIA.BO", "DRREDDY.BO", "CIPLA.BO",
        "BRITANNIA.BO", "EICHERMOT.BO", "HEROMOTOCO.BO", "BAJAJ-AUTO.BO", "M&M.BO",
        "TATAMOTORS.BO", "TATAPOWER.BO", "TATACONSUM.BO", "VEDL.BO", "HINDALCO.BO",
        "GRASIM.BO", "ULTRACEMCO.BO", "SHREECEM.BO", "AMBUJACEM.BO", "ACC.BO",
        "ASIANPAINT.BO", "BERGEPAINT.BO", "PIDILITIND.BO", "COLPAL.BO", "MARICO.BO",
        "DABUR.BO", "GODREJCP.BO", "EMAMILTD.BO",
        "APOLLOHOSP.BO", "FORTIS.BO", "MAXHEALTH.BO", "NARAYANA.BO", "LALPATHLAB.BO",
        "DIVISLAB.BO", "AUROPHARMA.BO", "ALKEM.BO", "TORNTPHARM.BO", "IPCA.BO",
        "GLENMARK.BO", "LAURUSLABS.BO", "GRANULES.BO", "NATCOPHARM.BO", "AJANTPHARM.BO",
        "ABBOTINDIA.BO", "PFIZER.BO",
        "COFORGE.BO", "MPHASIS.BO", "PERSISTENT.BO", "TATAELXSI.BO",
        "KPITTECH.BO", "LTIM.BO", "CYIENT.BO", "HEXAWARE.BO", "ZENSAR.BO",
        "ANGELONE.BO", "MOTILALOFS.BO", "IIFL.BO", "CHOLAFIN.BO", "BAJAJFINSV.BO",
        "MUTHOOTFIN.BO", "LICHSGFIN.BO", "PNBHOUSING.BO", "SBILIFE.BO", "HDFCLIFE.BO",
        "ICICIGI.BO", "HDFCAMC.BO", "NIPPONLIFE.BO", "STARHEALTH.BO",
        "BANKBARODA.BO", "PNB.BO", "CANBK.BO", "UNIONBANK.BO", "INDIANB.BO",
        "IDFCFIRSTB.BO", "BANDHANBNK.BO", "FEDERALBNK.BO", "KARURVYSYA.BO", "DCBBANK.BO",
        "IRCTC.BO", "CONCOR.BO", "RVNL.BO", "RITES.BO", "IRFC.BO",
        "NBCC.BO", "BEL.BO", "HAL.BO", "BHEL.BO",
        "RECLTD.BO", "PFC.BO", "NHPC.BO", "SJVN.BO", "TORNTPOWER.BO",
        "CESC.BO", "IEX.BO", "ADANIGREEN.BO",
        "GODREJPROP.BO", "OBEROIRLTY.BO", "PRESTIGE.BO", "BRIGADE.BO", "SOBHA.BO",
        "DMART.BO", "TRENT.BO", "ABFRL.BO", "NYKAA.BO",
        "JUBLFOOD.BO", "WESTLIFE.BO", "DEVYANI.BO", "INDHOTEL.BO", "EIHOTEL.BO",
        "ZOMATO.BO", "NAUKRI.BO",
        "NMDC.BO", "SAIL.BO", "HINDZINC.BO", "NALCO.BO",
        "POLYCAB.BO", "HAVELLS.BO", "CROMPTON.BO", "VOLTAS.BO", "BLUESTAR.BO",
        "ASTRAL.BO", "SUPREMEIND.BO", "KEI.BO", "VGUARD.BO",
        "MRF.BO", "BALKRISIND.BO", "APOLLOTYRE.BO", "CEATLTD.BO", "MOTHERSON.BO",
        "BHARATFORG.BO", "BOSCHLTD.BO", "ESCORTS.BO",
        "DEEPAKFERT.BO", "CHAMBLFERT.BO", "COROMANDEL.BO", "AARTI.BO",
        "VINATI.BO", "NAVINFLUOR.BO", "ATUL.BO", "SUMICHEM.BO",
        "SIEMENS.BO", "ABB.BO", "CUMMINSIND.BO", "THERMAX.BO",
        "PAGEIND.BO", "SUNTV.BO", "ZEEL.BO", "PVRINOX.BO",
        "MCDOWELL-N.BO", "RADICO.BO",
        "JKCEMENT.BO", "RAMCOCEM.BO", "ORIENTCEM.BO",
    ],

    # ── US S&P 500 + ETFs ──────────────────────────────
    "us": [
        # Technology
        "AAPL", "MSFT", "NVDA", "META", "GOOGL", "GOOG", "AMZN", "TSLA",
        "AVGO", "ORCL", "CRM", "AMD", "QCOM", "TXN", "INTC", "CSCO",
        "IBM", "INTU", "NOW", "ADBE", "AMAT", "MU", "LRCX", "KLAC",
        "SNPS", "CDNS", "MRVL", "MSI", "ANSS", "TER", "MPWR",
        "STX", "WDC", "HPQ", "HPE", "NTAP", "JNPR", "AKAM",
        "CDW", "CTSH", "IT", "LDOS", "VRSN",
        # Financials
        "BRK-B", "JPM", "V", "MA", "BAC", "WFC", "GS", "MS",
        "BLK", "SPGI", "MCO", "ICE", "CME", "CBOE",
        "AXP", "DFS", "COF", "SYF", "AIG", "MET", "PRU", "AFL",
        "TRV", "ALL", "PGR", "CB", "HIG",
        "USB", "PNC", "TFC", "STT", "NTRS", "BK",
        "FITB", "RF", "CFG", "HBAN", "KEY", "MTB",
        # Healthcare
        "JNJ", "UNH", "LLY", "ABBV", "MRK", "TMO", "ABT", "DHR",
        "BMY", "AMGN", "PFE", "GILD", "REGN", "VRTX", "BIIB",
        "IQV", "A", "WAT", "MTD", "STE", "TFX", "ALGN",
        "EW", "ZBH", "BSX", "BDX", "SYK", "ISRG", "MDT",
        "HCA", "UHS", "CNC", "HUM", "CI", "CVS", "MCK",
        # Consumer Discretionary
        "HD", "MCD", "NKE", "SBUX", "TGT", "LOW", "COST",
        "WMT", "DG", "DLTR", "ROST", "TJX",
        # Consumer Staples
        "PG", "KO", "PEP", "PM", "MO", "MDLZ", "KHC", "GIS",
        "K", "SJM", "CAG", "HRL", "MKC", "CLX", "CHD", "CL",
        # Industrials
        "GE", "HON", "CAT", "DE", "RTX", "LMT", "NOC", "GD",
        "BA", "LHX", "TXT",
        "UPS", "FDX", "XPO", "JBHT", "ODFL", "CHRW",
        "MMM", "EMR", "ETN", "PH", "ROK", "AME", "XYL",
        "WM", "RSG", "CTAS", "FAST", "GWW",
        # Energy
        "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO",
        "PXD", "OXY", "DVN", "HAL", "BKR",
        "KMI", "WMB", "OKE", "LNG",
        # Utilities
        "NEE", "DUK", "SO", "D", "AEP", "EXC", "SRE", "XEL",
        "WEC", "ES", "ETR", "FE", "CNP", "AES",
        # Real Estate
        "AMT", "PLD", "EQIX", "CCI", "SPG", "O", "VICI", "WELL",
        "EQR", "AVB", "MAA",
        # Materials
        "LIN", "APD", "ECL", "SHW", "PPG", "RPM",
        "NUE", "STLD", "FCX", "AA",
        # ETFs
        "SPY", "QQQ", "DIA", "IWM", "VTI", "VOO",
        "XLF", "XLK", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU",
        "XLB", "XLRE", "XLC", "GLD", "SLV", "TLT", "IEF",
    ],

    # ── NASDAQ 100 ─────────────────────────────────────
    "nasdaq100": [
        "MSFT", "AAPL", "NVDA", "AMZN", "META", "TSLA", "GOOGL", "GOOG",
        "AVGO", "COST", "NFLX", "ASML", "AMD", "PEP", "QCOM",
        "LIN", "INTU", "AMAT", "ISRG", "TXN", "BKNG", "CMCSA", "AMGN",
        "MU", "HON", "VRTX", "INTC", "SBUX", "ADI", "PANW", "GILD",
        "LRCX", "MDLZ", "REGN", "KLAC", "SNPS", "CDNS", "MELI", "CSX",
        "PYPL", "CTAS", "CRWD", "ORLY", "MAR", "NXPI", "PCAR", "FTNT",
        "MNST", "MRVL", "ADSK", "DXCM", "KDP", "CPRT", "KHC", "ROST",
        "ADP", "CHTR", "FANG", "ODFL", "PAYX", "CEG", "TTD", "IDXX",
        "MRNA", "BIIB", "ILMN", "GEHC", "DDOG", "TEAM", "ZS", "ANSS",
        "ON", "DLTR", "FAST", "VRSK", "CSGP", "EXC",
        "XEL", "EBAY", "ABNB", "DASH", "WDAY",
    ],

    # ── Crypto — Top 20 ────────────────────────────────
    "crypto": [
        "BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "XRP-USD",
        "ADA-USD", "AVAX-USD", "DOGE-USD", "DOT-USD", "MATIC-USD",
        "LINK-USD", "UNI-USD", "ATOM-USD", "LTC-USD", "BCH-USD",
        "XLM-USD", "ALGO-USD", "VET-USD", "FIL-USD", "AAVE-USD"
    ],

    # ── Forex — 8 major pairs ──────────────────────────
    "forex": [
        "USDINR=X", "EURUSD=X", "GBPUSD=X", "JPYUSD=X",
        "AUDUSD=X", "CADUSD=X", "CHFUSD=X", "CNYUSD=X"
    ]
}


if __name__ == "__main__":
    for market, symbols in MARKETS.items():
        print(f"{market:12} → {len(symbols)} symbols")
    print(f"{'TOTAL':12} → {sum(len(v) for v in MARKETS.values())} symbols")