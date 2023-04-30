import { setGraphBuilderData, final_nodes_edges } from "./panels/graph-builder";
import { insertResultForVariable } from "./utils";
import { buildTable } from "./panels/results-table";
import { addOrGetNode, addEdge } from "./utils";
import axios from 'axios';
import { setChartOutput } from "./panels/chart-output";

let outputElements;

async function getResults(dataset_choice) {
  let queryGraphData = final_nodes_edges();
  let nodes_data = [];
  let edges_data = [];

  await queryGraphData.nodes.forEach((node) => {
    nodes_data.push({
      id: node.id,
      label: node.value,
      props: node.props,
      retValue: "True",
    })
  });

  await queryGraphData.edges.forEach((edge) => {
    edges_data.push({ from: edge.source,
      to: edge.target,
      label: edge.value,
      props: edge.props,
    })
  });

  console.log("titty: ", nodes_data);

	const response = await axios
  .post("http://localhost:8080/query", {
    dataset: dataset_choice,
    nodes: nodes_data,
    // nodes: [
    //   {
    //     id: 1,
    //     label: "Artist",
    //     // props: [{ key: "Name", value: "Canela Cox", op: "eq" }],
    //     props: [{}],
    //     retValue: "True",
    //   },
    //   { id: 2, label: "Concert", props: [{}], retValue: "True" },
    // ],
    edges: [{ from: 0, to: 1, label: "Performed", props: [{}] }],
    // edges: edges_data
  });
  let data;
  console.log("Check", response);
  if(response.status === 200){
    data = response.data;
  }else{
    data = null;
  }
  console.log("Data is: ", data);
  return data;
};

const init_graph_ex = () => {
  let nodes = {};
  let edges = [];

  let subNode = addOrGetNode(nodes, { value: "Artist", termType: "Variable", props: [{}] });
  let objNode = addOrGetNode(nodes, { value: "Concert", termType: "Variable", props: [{}] });
  addEdge(
    edges,
    { value: "Performed", termType: "Variable", props: [{}] },
    subNode.id,
    objNode.id
  );

  return { prefixes: [], nodes: nodes, edges: edges };
};

const initModel = (_outputElements) => {
  outputElements = _outputElements;
  document
    .getElementById(outputElements.submitButtonId)
    .addEventListener("click", () => submitBtn());

  setGraphBuilderData(init_graph_ex()); // edge.source/target will be made the node objects instead of just ids
};

const submitBtn = async() => {
  let dataset_choice = null;
  if (outputElements.datasetChoiceToy.checked) {
    dataset_choice = "Toy";
  } else if (outputElements.datasetChoiceIMDB.checked) {
    dataset_choice = "IMDB";
  }
  if (dataset_choice == null) {
    alert("Please Select a Dataset");
    return;
  }
  console.log(dataset_choice);

  // POST Request
  let res = getResults(dataset_choice);
  let [data] = await Promise.all([res]);

  const runtime = data.runtime;
  data = data.results;
  console.log("API Results");
  console.log(data);

  outputElements.outputWrapperDiv.style.display = "flex";
  setChartOutput(runtime);

  // variables -> column header; rows -> row of table;

  let variables = final_nodes_edges().nodes.filter((n) => {
    return n.type === "Variable";
  });


  let c11 = {
    value: "J.Lo",
    props: {},
  };

  let c12 = {
    value: "Feed India",
    props: {},
  };

  let c21 = {
    value: "Chris Martin",
    props: {},
  };

  let c22 = {
    value: "Global Concert",
    props: {},
  };

  let rows = [
    // { Artist: c11, Concert: c12 },
    // { Artist: c21, Concert: c22 },
  ];

  for(let r = 0; r < data.length; r++){
    const map1 = new Map();
    for(let c = 0; c<data[0].length; c++){
      map1.set(data[r][c][0], data[r][c][1]);
    }
    rows.push(map1);
  }

  let prefixes = [];

  console.log("query result:", variables, rows);
  console.log("query result row:", rows);


  buildTable(variables, rows, prefixes, (selectedRow) => {
    let queryGraphData = final_nodes_edges();
    console.log("nodes and edges", queryGraphData);

    if (selectedRow) {
      queryGraphData.nodes.forEach((node) => {
        insertResultForVariable(node, selectedRow);
      });
      console.log("queryGraphData", queryGraphData);
    }
  });
};

export { initModel };
