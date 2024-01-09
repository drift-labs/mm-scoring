import {DataFrame} from "danfojs-node";

export function getFormattedLpSharesDataFrame(json: any) {
	const perpMarkets = json["perpMarketAccounts"];
	const perpMarketSqrtK = new Map<number, string>();

	for (const perpMarket of perpMarkets) {
		const marketIndex = perpMarket["marketIndex"];
		const sqrtK = perpMarket["amm"]["sqrtK"];
		perpMarketSqrtK.set(marketIndex, sqrtK);
	}

	const slot = json["slot"];

	const lps = new Map<number, any>()
	for (const userLp of json["userLpPositions"]) {
		const marketIndex = Number(userLp["position"]["marketIndex"]);
		const sqrtK = perpMarketSqrtK.get(Number(userLp["position"]["marketIndex"]));
		const lp = {
			user: userLp["user"],
			slot,
			lpShares: userLp["position"]["lpShares"],
			sqrtK,
		}
		if (lps.has(marketIndex)) {
			lps.get(marketIndex).push(lp);
		} else {
			lps.set(marketIndex, [lp]);
		}
	}

	const lpDataFrames = new Map<number, DataFrame>();
	for (const [marketIndex, lpList] of lps) {
		const lpDataFrame = new DataFrame(lpList);
		lpDataFrames.set(marketIndex, lpDataFrame);
	}

	return lpDataFrames;
}

export function getDefaultAggregateLpShares() {
	return new DataFrame([], {columns: ["user", "slot", "lpShares", "sqrtK"]});
}