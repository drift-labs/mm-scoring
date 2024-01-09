import {DataFrame} from "danfojs-node";
import * as dfd from "danfojs-node";
import * as Papa from "papaparse"

export function mergeDataframes(original: DataFrame, append: DataFrame) {
	if (original.size === 0 && append.size === 0) {
		return original;
	} else if (original.size === 0) {
		return append;
	} else if (append.size === 0) {
		return original;
	} else {
		return dfd.concat({ dfList: [original, append], axis: 0 });
	}
}

export function getDataFrameFromString(csv: string) : DataFrame {
	const data = Papa.parse(csv).data;
	const filteredData = data.slice(1).filter((row: any) => row.length === 3);
	return new DataFrame(filteredData, {columns: data[0]});
}