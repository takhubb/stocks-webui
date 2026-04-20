const charts = {};

const metricBlueprint = [
  { key: "industry", label: "業種", format: "text", subtext: "" },
  { key: "selfCapitalRatio", label: "自己資本比率", format: "percent", subtext: "" },
  { key: "roe", label: "ROE", format: "percent", subtext: "TTM ベース" },
  { key: "roa", label: "ROA", format: "percent", subtext: "TTM ベース" },
  { key: "peg", label: "PEG レシオ", format: "ratio", subtext: "予想 EPS 成長率ベース" },
  { key: "latestClose", label: "最新終値", format: "yen", subtext: "" },
  { key: "latestMarketCap", label: "最新時価総額", format: "compactYen", subtext: "" },
  { key: "industryAvgPSR", label: "業種平均 PSR", format: "ratio", subtextKey: "industryPSRCount" },
  { key: "industryAvgPER", label: "業種平均 PER", format: "ratio", subtextKey: "industryPERCount" },
];

const datasetPalette = {
  sales: { label: "売上高", color: "#c85b2f", fill: "rgba(200, 91, 47, 0.24)" },
  op: { label: "営業利益", color: "#1e6f66", fill: "rgba(30, 111, 102, 0.2)" },
  odp: { label: "経常利益", color: "#355f8d", fill: "rgba(53, 95, 141, 0.2)" },
  np: { label: "純利益", color: "#8b7dd8", fill: "rgba(139, 125, 216, 0.2)" },
  psr: { label: "PSR", color: "#c85b2f" },
  per: { label: "PER", color: "#1e6f66" },
  pbr: { label: "PBR", color: "#355f8d" },
  close: { label: "終値", color: "#16283a" },
  ma25: { label: "25週移動平均", color: "#c85b2f" },
  ma50: { label: "50週移動平均", color: "#1e6f66" },
  marketCap: { label: "時価総額", color: "#355f8d" },
  volume: { label: "出来高", color: "#c85b2f" },
};

const form = document.getElementById("analyze-form");
const codeInput = document.getElementById("code-input");
const submitButton = document.getElementById("submit-button");
const statusEl = document.getElementById("status");
const companyCard = document.getElementById("company-card");
const metricsGrid = document.getElementById("metrics-grid");
const chartsGrid = document.getElementById("charts-grid");
const notesCard = document.getElementById("notes-card");
const notesList = document.getElementById("notes-list");

codeInput.value = "7203";

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await analyze();
});

function setStatus(message, type) {
  statusEl.textContent = message;
  statusEl.className = `status ${type}`;
}

function formatCompact(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return new Intl.NumberFormat("ja-JP", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatMetric(value, format) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }

  switch (format) {
    case "percent":
      return `${value.toFixed(2)}%`;
    case "ratio":
      return value.toFixed(2);
    case "yen":
      return `${new Intl.NumberFormat("ja-JP").format(Math.round(value))} 円`;
    case "compactYen":
      return `${formatCompact(value)}円`;
    case "text":
      return value;
    default:
      return String(value);
  }
}

function lineDatasets(series, keys) {
  return keys
    .map((key) => {
      const palette = datasetPalette[key];
      const data = series[key];
      if (!data || data.every((value) => value === null || Number.isNaN(value))) {
        return null;
      }
      return {
        label: palette.label,
        data,
        borderColor: palette.color,
        backgroundColor: palette.fill || palette.color,
        tension: 0.25,
        borderWidth: 2.4,
        pointRadius: 0,
        spanGaps: true,
      };
    })
    .filter(Boolean);
}

function barDatasets(series, keys) {
  return keys
    .map((key) => {
      const palette = datasetPalette[key];
      const data = series[key];
      if (!data || data.every((value) => value === null || Number.isNaN(value))) {
        return null;
      }
      return {
        label: palette.label,
        data,
        borderColor: palette.color,
        backgroundColor: palette.fill || palette.color,
        borderRadius: 8,
        borderWidth: 1,
      };
    })
    .filter(Boolean);
}

function baseOptions(valueType = "plain", showLegend = true) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: "index",
      intersect: false,
    },
    plugins: {
      legend: {
        display: showLegend,
        labels: {
          usePointStyle: true,
          boxWidth: 8,
          padding: 18,
          color: "#16283a",
        },
      },
      tooltip: {
        callbacks: {
          label(context) {
            const value = context.parsed.y;
            if (value === null || value === undefined || Number.isNaN(value)) {
              return `${context.dataset.label}: N/A`;
            }
            if (valueType === "percent") {
              return `${context.dataset.label}: ${value.toFixed(2)}%`;
            }
            if (valueType === "compact-yen" || valueType === "compact") {
              return `${context.dataset.label}: ${formatCompact(value)}`;
            }
            return `${context.dataset.label}: ${value.toFixed(2)}`;
          },
        },
      },
    },
    scales: {
      x: {
        grid: {
          color: "rgba(22, 40, 58, 0.06)",
        },
        ticks: {
          color: "#617487",
          maxRotation: 0,
          autoSkip: true,
        },
      },
      y: {
        grid: {
          color: "rgba(22, 40, 58, 0.08)",
        },
        ticks: {
          color: "#617487",
          callback(value) {
            if (valueType === "percent") {
              return `${value}%`;
            }
            if (valueType === "compact-yen" || valueType === "compact") {
              return formatCompact(value);
            }
            return value;
          },
        },
      },
    },
  };
}

function renderChart(canvasId, config) {
  if (charts[canvasId]) {
    charts[canvasId].destroy();
  }

  const context = document.getElementById(canvasId);
  charts[canvasId] = new Chart(context, config);
}

function renderCompany(company) {
  document.getElementById("company-code").textContent = company.code;
  document.getElementById("company-name").textContent = company.name || "N/A";
  document.getElementById("company-name-en").textContent = company.nameEn || "";
  document.getElementById("company-market").textContent = company.market || "N/A";
  document.getElementById("company-industry").textContent = company.industry33 || "N/A";
  document.getElementById("company-price-date").textContent = company.latestPriceDate || "N/A";
  document.getElementById("company-disclosure-date").textContent = company.lastDisclosureDate || "N/A";
  companyCard.classList.remove("hidden");
}

function renderMetrics(metrics) {
  metricsGrid.innerHTML = metricBlueprint
    .map((metric) => {
      const value = formatMetric(metrics[metric.key], metric.format);
      let subtext = metric.subtext || "";
      if (metric.subtextKey) {
        const count = metrics[metric.subtextKey];
        subtext = count ? `算出銘柄数 ${count}` : "算出不可";
      }
      return `
        <article class="metric-card">
          <p class="metric-label">${metric.label}</p>
          <p class="metric-value">${value}</p>
          <p class="metric-subtext">${subtext}</p>
        </article>
      `;
    })
    .join("");
  metricsGrid.classList.remove("hidden");
}

function renderNotes(notes) {
  if (!notes || notes.length === 0) {
    notesCard.classList.add("hidden");
    notesList.innerHTML = "";
    return;
  }
  notesList.innerHTML = notes.map((note) => `<li>${note}</li>`).join("");
  notesCard.classList.remove("hidden");
}

function renderCharts(chartData) {
  renderChart("weekly-price-chart", {
    type: "line",
    data: {
      labels: chartData.weeklyPrice.labels,
      datasets: lineDatasets(chartData.weeklyPrice.series, ["close", "ma25", "ma50"]),
    },
    options: baseOptions("plain"),
  });

  renderChart("volume-chart", {
    type: "line",
    data: {
      labels: chartData.weeklyVolume.labels,
      datasets: lineDatasets(chartData.weeklyVolume.series, ["volume"]),
    },
    options: baseOptions("compact", false),
  });

  renderChart("market-cap-chart", {
    type: "line",
    data: {
      labels: chartData.weeklyMarketCap.labels,
      datasets: lineDatasets(chartData.weeklyMarketCap.series, ["marketCap"]),
    },
    options: baseOptions("compact-yen", false),
  });

  renderChart("psr-chart", {
    type: "line",
    data: {
      labels: chartData.valuation.labels,
      datasets: lineDatasets(chartData.valuation.series, ["psr"]),
    },
    options: baseOptions("plain", false),
  });

  renderChart("per-chart", {
    type: "line",
    data: {
      labels: chartData.valuation.labels,
      datasets: lineDatasets(chartData.valuation.series, ["per"]),
    },
    options: baseOptions("plain", false),
  });

  renderChart("pbr-chart", {
    type: "line",
    data: {
      labels: chartData.valuation.labels,
      datasets: lineDatasets(chartData.valuation.series, ["pbr"]),
    },
    options: baseOptions("plain", false),
  });

  renderChart("sales-chart", {
    type: "bar",
    data: {
      labels: chartData.profits.labels,
      datasets: barDatasets(chartData.profits.series, ["sales"]),
    },
    options: baseOptions("compact-yen", false),
  });

  renderChart("op-chart", {
    type: "bar",
    data: {
      labels: chartData.profits.labels,
      datasets: barDatasets(chartData.profits.series, ["op"]),
    },
    options: baseOptions("compact-yen", false),
  });

  renderChart("odp-chart", {
    type: "bar",
    data: {
      labels: chartData.profits.labels,
      datasets: barDatasets(chartData.profits.series, ["odp"]),
    },
    options: baseOptions("compact-yen", false),
  });

  renderChart("np-chart", {
    type: "bar",
    data: {
      labels: chartData.profits.labels,
      datasets: barDatasets(chartData.profits.series, ["np"]),
    },
    options: baseOptions("compact-yen", false),
  });

  renderChart("yoy-chart", {
    type: "line",
    data: {
      labels: chartData.yoy.labels,
      datasets: lineDatasets(chartData.yoy.series, ["sales", "op", "odp", "np"]),
    },
    options: baseOptions("percent"),
  });

  chartsGrid.classList.remove("hidden");
}

async function analyze() {
  const code = codeInput.value.trim();
  if (!code) {
    setStatus("銘柄コードを入力してください。", "error");
    return;
  }

  submitButton.disabled = true;
  setStatus("J-Quants からデータを取得しています。初回は bulk キャッシュ構築で少し時間がかかります。", "loading");

  try {
    const response = await fetch(`/api/analyze?code=${encodeURIComponent(code)}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || "分析に失敗しました。");
    }

    renderCompany(payload.company);
    renderMetrics(payload.metrics);
    renderNotes(payload.notes);
    renderCharts(payload.charts);
    setStatus("分析が完了しました。", "success");
  } catch (error) {
    setStatus(error.message || "分析に失敗しました。", "error");
  } finally {
    submitButton.disabled = false;
  }
}
