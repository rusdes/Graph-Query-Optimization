import { initGraphBuilder } from './panels/graph-builder';
import { initResultsTable } from "./panels/results-table";
import { initChartOutput } from './panels/chart-output';
import { initModel } from "./model";

window.init = config => {
    initGraphBuilder(config.graphBuilder);
    initResultsTable(config.resultsTable);
    initChartOutput(config.chart);
    initModel(config.outputElements);
};
