let div;

const setChartOutput = (runtime_data) => {
  var data = [
    {
      x: [
        "VNEN",
        "VNEKUbal",
        "VNEKBal",
        "VKENUbal",
        "VKENBal",
        "VKEKUbal",
        "VKEKBal",
      ],
      y: runtime_data,
      type: "bar",
    },
  ];

  var layout = {
    title: "Runtime Comparison",
    xaxis: {
      title: "Models",
      titlefont: {
        size: 16,
        color: "rgb(107, 107, 107)",
      },
      tickfont: {
        size: 14,
        color: "rgb(107, 107, 107)",
      },
    },
    yaxis: {
      title: "Time (ms)",
      titlefont: {
        size: 16,
        color: "rgb(107, 107, 107)",
      },
      tickfont: {
        size: 14,
        color: "rgb(107, 107, 107)",
      },
    },
    showlegend: false,
  };

  Plotly.newPlot(div, data, layout, { displayModeBar: false });
};
const initChartOutput = (config) => {
  div = config.div;
};

export { initChartOutput, setChartOutput };
