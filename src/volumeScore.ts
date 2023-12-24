import * as dfd from "danfojs-node"
import {DataFrame, Series} from "danfojs-node";

export function getVolumeScore(trades: DataFrame) {
	const tradesCopy = trades.copy();
	tradesCopy.replace("undefined", "vAMM", { columns: ["taker"], inplace: true} );
	tradesCopy.replace("undefined", "vAMM", { columns: ["maker"], inplace: true} );
	const makerVolume = tradesCopy.groupby(["maker"]).agg({"quoteAssetAmountFilled": "sum"});
	makerVolume.rename({"maker": "User", "quoteAssetAmountFilled_sum": "MakerVolume"}, {inplace: true})
	makerVolume.fillNa(0, { inplace: true });
	const takerVolume = tradesCopy.groupby(["taker"]).agg({"quoteAssetAmountFilled": "sum"});
	takerVolume.rename({"taker": "User", "quoteAssetAmountFilled_sum": "TakerVolume"}, {inplace: true})
	takerVolume.fillNa(0, { inplace: true });

	makerVolume.print();
	takerVolume.print();

	const merged = dfd.merge({
		left: makerVolume,
		right: takerVolume,
		on: ["User"],
		how: "inner"
	});

	merged.addColumn("TotalVolume", merged["MakerVolume"].add(merged["TakerVolume"]), { inplace: true });
	merged.sortValues("TotalVolume", {ascending: false, inplace: true});
	merged.setIndex({column: "User", inplace: true});
	merged.drop({columns: ["User"], inplace: true});

	return merged;
}