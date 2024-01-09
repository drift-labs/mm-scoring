import * as dfd from "danfojs-node"
import {DataFrame, Series} from "danfojs-node";
import {BASE_PRECISION, BN, convertToNumber, PRICE_PRECISION, QUOTE_PRECISION} from "@drift-labs/sdk";
import * as Papa from "papaparse"

export function getFormattedOrderDataFrame(json: any) {
	const orders = json["orders"];

	const flattenedOrders = [];

	for (const order of orders) {
		const orderInfo = order["order"];
		orderInfo["baseAssetAmount"] = convertToNumber(new BN(orderInfo["baseAssetAmount"]), BASE_PRECISION);
		orderInfo["baseAssetAmountFilled"] = convertToNumber(new BN(orderInfo["baseAssetAmountFilled"]), BASE_PRECISION);
		orderInfo["price"] = convertToNumber(new BN(orderInfo["price"]), PRICE_PRECISION);
		orderInfo["oraclePriceOffset"] = convertToNumber(new BN(orderInfo["oraclePriceOffset"]), PRICE_PRECISION);

		if (orderInfo["oraclePriceOffset"] != 0) {
			let oraclePrice = getOraclePrice(json, orderInfo["marketType"], orderInfo["marketIndex"]);
			orderInfo["price"] = oraclePrice + orderInfo["oraclePriceOffset"];
		}

		orderInfo["user"] = order["user"];

		flattenedOrders.push(orderInfo);
	}

	const slot = json["slot"];
	const vammLiquidity = json["vammLiquidity"];
	for (const marketLiquidity of vammLiquidity) {
		const marketIndex = marketLiquidity["marketIndex"];
		for (const ask of marketLiquidity["asks"]) {
			const order = {
				status: "open",
				orderType: 'limit',
				marketType: "perp",
				slot,
				orderId: 0,
				userOrderId: 0,
				marketIndex,
				baseAssetAmount: convertToNumber(new BN(ask['size']), BASE_PRECISION),
				baseAssetAmountFilled: 0,
				quoteAssetAmountFilled: '0',
				price: convertToNumber(new BN(ask['price']), PRICE_PRECISION),
				direction: 'short',
				reduceOnly: false,
				triggerPrice: '0',
				triggerCondition: 'above',
				existingPositionDirection: 'long',
				postOnly: false,
				immediateOrCancel: false,
				oraclePriceOffset: 0,
				auctionDuration: 0,
				auctionStartPrice: '0',
				auctionEndPrice: '0',
				maxTs: '0',
				user: 'vamm'
			}
			flattenedOrders.push(order);
		}

		for (const bid of marketLiquidity["bids"]) {
			const order = {
				status: "open",
				orderType: 'limit',
				marketType: "perp",
				slot,
				orderId: 0,
				userOrderId: 0,
				marketIndex,
				baseAssetAmount: convertToNumber(new BN(bid['size']), BASE_PRECISION),
				baseAssetAmountFilled: 0,
				quoteAssetAmountFilled: '0',
				price: convertToNumber(new BN(bid['price']), PRICE_PRECISION),
				direction: 'long',
				reduceOnly: false,
				triggerPrice: '0',
				triggerCondition: 'above',
				existingPositionDirection: 'long',
				postOnly: false,
				immediateOrCancel: false,
				oraclePriceOffset: 0,
				auctionDuration: 0,
				auctionStartPrice: '0',
				auctionEndPrice: '0',
				maxTs: '0',
				user: 'vamm'
			}
			flattenedOrders.push(order);
		}
	}

	return new DataFrame(flattenedOrders);
}

export function getOraclePrice(json: any, marketType: string, marketIndex: number) {
	if (marketType === "spot") {
		return convertToNumber(new BN(json["spotOracles"][marketIndex]["price"]), QUOTE_PRECISION);
	} else {
		return convertToNumber(new BN(json["perpOracles"][marketIndex]["price"]), QUOTE_PRECISION);
	}
}

export function getLiquidityScoreForSnapshot(df: DataFrame, marketType: string, marketIndex: number, oraclePrice: number, slot: number) {
	const d = df.query(df["orderType"].eq("limit").and(df["marketIndex"].eq(marketIndex)).and(df["marketType"].eq(marketType)));
	d.resetIndex({inplace: true});
	d.addColumn("currentSlot", d.apply(_ => slot, {axis: 1}) as Series, {inplace: true});
	d.query(d["auctionDuration"].eq(0).or(d["currentSlot"].sub(d["slot"]).gt(d["auctionDuration"])), {inplace: true});
	d.resetIndex({inplace: true});

	d.addColumn("baseAssetAmountLeft", d["baseAssetAmount"].sub(d["baseAssetAmountFilled"]), { inplace: true });

	let bestBid = d.query(d["direction"].eq("long"))["price"].max();
	let bestAsk = d.query(d["direction"].eq("short"))["price"].min();

	if (!bestBid) {
		bestBid = oraclePrice;
	}

	if (!bestAsk) {
		bestAsk = oraclePrice;
	}

	if (bestBid > bestAsk) {
		if (bestBid > oraclePrice) {
			bestBid = bestAsk
		} else {
			bestAsk = bestBid
		}
	}

	const markPrice = (bestBid + bestAsk) / 2

	const mbpsValues = [
		0.00025,
		0.0005,
		0.001,
		0.002,
		0.003,
		0.004
	];
	function roundThreshold(x, direction) {
		let withinBpsOfPrice = .0005
		if (direction === "long") {
			for (let i = 0; i < mbpsValues.length; i++) {
				if (x >= bestBid * (1- mbpsValues[i])) {
					withinBpsOfPrice = markPrice * mbpsValues[i]
					break
				}
			}
			return Math.floor( x / withinBpsOfPrice) * withinBpsOfPrice
		} else {
			for (let i = 0; i < mbpsValues.length; i++) {
				if (x <= bestAsk * (1 + mbpsValues[i])) {
					withinBpsOfPrice = markPrice * mbpsValues[i]
					break
				}
			}
			return Math.ceil(x / withinBpsOfPrice) * withinBpsOfPrice
		}
	}

	const priceRoundedSeries = d.loc({columns: ["price", "direction"]}).apply(([x, y]) => roundThreshold(x, y), {axis: 1}) as Series;
	d.addColumn("priceRounded", priceRoundedSeries, {inplace: true});

	d.addColumn("level", d.apply(_ => NaN, {axis: 1}) as Series, {inplace: true});
	d.addColumn("score", d.apply(_ => NaN, {axis: 1}) as Series, {inplace: true});

	const top6BidsQuery = d.query(d["direction"].eq("long"));
	let top6Bids = new DataFrame([], {columns: ["priceRounded", "baseAssetAmountLeft_sum"]});
	if (top6BidsQuery.size) {
		// @ts-ignore
		top6Bids = d.query(d["direction"].eq("long")).groupby(["priceRounded"]).agg({"baseAssetAmountLeft": "sum"}).sortValues("priceRounded", {ascending: false});
	}
	const top6AsksQuery = d.query(d["direction"].eq("short"));
	let top6Asks = new DataFrame([], {columns: ["priceRounded", "baseAssetAmountLeft_sum"]});
	if (top6AsksQuery.size) {
		// @ts-ignore
		top6Asks = d.query(d["direction"].eq("short")).groupby(["priceRounded"]).agg({"baseAssetAmountLeft": "sum"}).sortValues("priceRounded", {ascending: true});
	}

	const tts = dfd.concat({ dfList: [top6Bids.column("baseAssetAmountLeft_sum"), top6Asks.column("baseAssetAmountLeft_sum")], axis: 1}) as DataFrame;
	tts.fillNa(0, {inplace: true});
	tts.$setColumnNames(["bs", "as"]);

	const minq = 5000/markPrice;

	const q = tts.column("bs").add(tts.column("as")).div(2).apply(x => Math.max(x, minq)).max();

	const scoreScale = tts.min({axis: 1}).div(q).mul(100);

	let multipliers = new Array(scoreScale.size).fill(0);
	const tiersMultiplier = [8, 4, .75, .4, .3, .1];
	for (let i = 0; i < Math.min(tiersMultiplier.length, multipliers.length); i++) {
		multipliers[i] = tiersMultiplier[i];
	}

	scoreScale.mul(multipliers, { inplace: true });

	const chars = ['A', 'B', 'C', 'D', 'E', 'F'];

	//@ts-ignore
	for (const [i, [price]] of top6Bids.values.entries()) {
		const char = chars[i];
		if (!char) {
			continue;
		}
		const ba = d.query(d["priceRounded"].eq(price).and(d["direction"].eq("long"))).column("baseAssetAmountLeft");
		ba.div(ba.sum(), { inplace: true });
		ba.mul(scoreScale.values[i] as number, { inplace: true });

		const scoreColumnIndex = d.columns.indexOf("score");
		const levelColumnIndex = d.columns.indexOf("level");
		for (const [baIndex, dRowIndex] of ba.index.entries()) {
			d.values[dRowIndex][scoreColumnIndex] = ba.values[baIndex];
			d.values[dRowIndex][levelColumnIndex] = char +'-bid';
		}
	}

	//@ts-ignore
	for (const [i, [price]] of top6Asks.values.entries()) {
		const char = chars[i];
		if (!char) {
			continue;
		}
		const ba = d.query(d["priceRounded"].eq(price).and(d["direction"].eq("short"))).column("baseAssetAmountLeft");
		ba.div(ba.sum(), { inplace: true });
		ba.mul(scoreScale.values[i] as number, { inplace: true });

		const scoreColumnIndex = d.columns.indexOf("score");
		const levelColumnIndex = d.columns.indexOf("level");
		for (const [baIndex, dRowIndex] of ba.index.entries()) {
			d.values[dRowIndex][scoreColumnIndex] = ba.values[baIndex];
			d.values[dRowIndex][levelColumnIndex] = char +'-ask';
		}
	}

	d.addColumn("oraclePrice", d.apply(_ => oraclePrice, {axis: 1}) as Series, {inplace: true});

	return d;
}

export function getDefaultAggregateLiquidityScores() {
	return new DataFrame([], {columns: ["user", "score", "slot"]});
}

export function groupLiquidityScoreForAggregateList(df: DataFrame, slot: number) {
	df.fillNa(0, {columns: ["score"], inplace: true});
	const aggQuery = df.query(df["score"].gt(0));
	if (!aggQuery.size) {
		return getDefaultAggregateLiquidityScores();
	}
	const aggregated = aggQuery.groupby(["user"]).agg({"score": "sum"});
	aggregated.$setColumnNames(["user", "score"]);
	aggregated.addColumn("slot", aggregated.apply(_ => slot, {axis: 1}) as Series, {inplace: true});

	return aggregated;
}