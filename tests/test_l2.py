import pandas as pd

try:
    test_df = pd.read_csv("data/L2_DOGE_USD_orderbook.csv")
except:
    test_df = pd.read_csv("tests/data/L2_DOGE_USD_orderbook.csv")

ap_cols = ["ap0", "ap1", "ap2", "ap3", "ap4", "ap5", "ap6", "ap7", "ap8", "ap9"]
av_cols = ["av0", "av1", "av2", "av3", "av4", "av5", "av6", "av7", "av8", "av9"]
bp_cols = ["bp0", "bp1", "bp2", "bp3", "bp4", "bp5", "bp6", "bp7", "bp8", "bp9"]
bv_cols = ["bv0", "bv1", "bv2", "bv3", "bv4", "bv5", "bv6", "bv7", "bv8", "bv9"]


def test_price_ordering():
    fail = False
    for idx, row in test_df.iterrows():
        for i in range(9):
            if row[ap_cols[i]] > row[ap_cols[i+1]]:
                fail = True
                failType = "ask ordering"
                break
            if row[bp_cols[i]] < row[bp_cols[i+1]]:
                fail = True
                failType = "bid ordering"
                break

    assert not fail, f"Failed L2 book due to: {failType}."






