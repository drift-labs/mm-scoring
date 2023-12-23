import * as dfd from "danfojs-node"
import {DataFrame, Series} from "danfojs-node";

function getMMScoreForSnapSlot(df: DataFrame) {
	const d = df.query(df["orderType"].eq("limit"));
	d.addColumn("baseAssetAmountLeft", d["baseAssetAmount"].sub(d["baseAssetAmountFilled"]), { inplace: true });

	const oraclePrice = d["oraclePrice"].max();
	let bestBid = d.query(d["direction"].eq("long"))["price"].max();
	let bestAsk = d.query(d["direction"].eq("short"))["price"].min();

	if (bestBid > bestAsk) {
		if (bestBid > oraclePrice) {
			bestBid = bestAsk
		} else {
			bestAsk = bestBid
		}
	}

	console.log(bestBid, bestAsk)

	const markPrice = (bestBid + bestAsk) / 2

	const mbpsValues = [
		0.0001,
		0.0005,
		0.001,
		0.002,
		0.005,
		0.01
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

	const top6Bids = d.query(d["direction"].eq("long")).groupby(["priceRounded"]).agg({"baseAssetAmountLeft": "sum"}).sortValues("priceRounded", {ascending: false}).iloc({rows: ["0:6"]});
	const top6Asks = d.query(d["direction"].eq("short")).groupby(["priceRounded"]).agg({"baseAssetAmountLeft": "sum"}).sortValues("priceRounded", {ascending: true}).iloc({rows: ["0:6"]});

	const tts = dfd.concat({ dfList: [top6Bids.column("baseAssetAmountLeft_sum"), top6Asks.column("baseAssetAmountLeft_sum")], axis: 1}) as DataFrame;
	tts.$setColumnNames(["bs", "as"]);

	const minq = 5000/markPrice;

	const q = tts.column("bs").add(tts.column("as")).div(2).apply(x => Math.max(x, minq)).max();

	const scoreScale = tts.min({axis: 1}).div(q).mul(100);

	const multipliers = [4, 2, .75, .4, .3, .2];

	scoreScale.mul(multipliers, { inplace: true });

	const chars = ['A', 'B', 'C', 'D', 'E', 'F'];

	//@ts-ignore
	for (const [i, [price]] of top6Bids.values.entries()) {
		const char = chars[i];
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

	return d;
}