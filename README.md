# iceberg_recovery
Code to restore hidden parts for limit orders, join chains of limit orders to single icebergs, restore limit order book with hidden volume
hid_part.py
Script restores hidden part for ordinary limit orders submission in OrderLog from MOEX. "hid" column added to original OrderLog.
Also "sure" column added to measure possibility of additional hidden volume present (if sure=0). If sure=1 hidden part restored completly.
Augmented OrderLog will bw saved

Join_chains.py
Script combine chains of limit orders with hid>0 to single iceberg orders. Augmented OrderLog will be saved

LOB_recovery.py
Script calculates limit order book for particular moment of time with consideration of hidden volume in it.

