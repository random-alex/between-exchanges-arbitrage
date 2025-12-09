# OKX

Info about contracts from [OKX](https://www.okx.com/docs-v5/en/#overview-general-info)

instFamily and uly parameter explanation:

The following explanation is based on the BTC contract, other contracts are similar.
uly is the index, like "BTC-USD", and there is a one-to-many relationship with the settlement and margin currency (settleCcy).
instFamily is the trading instrument family, like BTC-USD_UM, and there is a one-to-one relationship with the settlement and margin currency (settleCcy).
The following table shows the corresponding relationship of uly, instFamily, settleCcy and instId.

Contract Type	uly	instFamily	settleCcy	Delivery contract instId	Swap contract instId
USDT-margined contract	BTC-USDT	BTC-USDT	USDT	BTC-USDT-250808	BTC-USDT-SWAP
USDC-margined contract	BTC-USDC	BTC-USDC	USDC	BTC-USDC-250808	BTC-USDC-SWAP
USD-margined contract	BTC-USD	BTC-USD_UM	USDⓈ	BTC-USD_UM-250808	BTC-USD_UM-SWAP
Coin-margined contract	BTC-USD	BTC-USD	BTC	BTC-USD-250808	BTC-USD-SWAP
Note:
1. USDⓈ represents USD and multiple USD stable coins, like USDC, USDG.
2. The settlement and margin currency refers to the settleCcy field returned by the Get instruments endpoint.
