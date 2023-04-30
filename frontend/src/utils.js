const addOrGetNode = (nodes, subOrObj, markNew = false) => {
    if (!nodes[subOrObj.value]) {
        nodes[subOrObj.value] = { id: Object.keys(nodes).length, value: subOrObj.value, type: subOrObj.termType, children: [], paths: [] , props: subOrObj.props};
        if (markNew) nodes[subOrObj.value].isNewInConstruct = true;
    }
    return nodes[subOrObj.value];
};

const addEdge = (edges, predicate, subNodeId, objNodeId, markNew = false) => {
    let value = predicate.value;
    let edge = { id: edges.length, source: subNodeId, target: objNodeId, value: value, type: predicate.termType, props: predicate.props};
    if (markNew) edge.isNewInConstruct = true;
    edges.push(edge);
};

const insertResultForVariable = (nodeOrEdge, resultRow) => {
    if (nodeOrEdge.type !== "Variable") return;
    nodeOrEdge.type = resultRow[nodeOrEdge.value].termType;
    nodeOrEdge.valueAsVariable = nodeOrEdge.value;
    nodeOrEdge.value = resultRow[nodeOrEdge.value].value;
    nodeOrEdge.wasVariable = true;
};

const buildShortFormIfPrefixExists = (prefixes, fullUri) => {
    let ret = fullUri;
    Object.entries(prefixes).forEach(([short, uri]) => {
        if (fullUri.startsWith(uri)) {
            ret = short + ":" + fullUri.substr(uri.length);
        }
    });
    return ret;
};

const orderNodesArray = unordered => {
    // are there less costly ways of doing this?
    let ordered = [];
    for (let i = 0; i < unordered.length; i++) {
        ordered.push(unordered.find(node => node.id === i));
    }
    return ordered;
};

export { insertResultForVariable, buildShortFormIfPrefixExists, orderNodesArray, addOrGetNode, addEdge }