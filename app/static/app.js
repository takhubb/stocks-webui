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
  sales: { label: "売上高", color: "#6f7cff", fill: "rgba(111, 124, 255, 0.32)" },
  op: { label: "営業利益", color: "#4ea6a6", fill: "rgba(78, 166, 166, 0.3)" },
  odp: { label: "経常利益", color: "#8796ff", fill: "rgba(135, 150, 255, 0.28)" },
  np: { label: "純利益", color: "#262b44", fill: "rgba(38, 43, 68, 0.24)" },
  psr: { label: "PSR", color: "#6f7cff" },
  per: { label: "PER", color: "#4ea6a6" },
  pbr: { label: "PBR", color: "#262b44" },
  roe: { label: "ROE", color: "#6f7cff" },
  roa: { label: "ROA", color: "#4ea6a6" },
  industry: { label: "業種平均", color: "#7a88a6" },
  topix: { label: "TOPIX (100=起点)", color: "#d46e5b" },
  close: { label: "終値", color: "#1f2740", fill: "rgba(31, 39, 64, 0.24)" },
  ma25: { label: "25週移動平均", color: "#6f7cff" },
  ma50: { label: "50週移動平均", color: "#4ea6a6" },
  marketCap: { label: "時価総額", color: "#262b44" },
  volume: { label: "出来高", color: "#6f7cff" },
};

const form = document.getElementById("analyze-form");
const codeInput = document.getElementById("code-input");
const searchClearButton = document.getElementById("search-clear-button");
const searchSuggestions = document.getElementById("search-suggestions");
const submitButton = document.getElementById("submit-button");
const statusEl = document.getElementById("status");
const companyCard = document.getElementById("company-card");
const metricsGrid = document.getElementById("metrics-grid");
const chartsGrid = document.getElementById("charts-grid");
const notesCard = document.getElementById("notes-card");
const notesList = document.getElementById("notes-list");

let searchDebounceTimer = null;
let searchAbortController = null;
let latestSuggestions = [];
let activeSuggestionIndex = -1;
let selectedSuggestion = null;
let isComposingSearch = false;

codeInput.value = "7203";
syncSearchClearButton();

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await analyze();
});

codeInput.addEventListener("compositionstart", () => {
  isComposingSearch = true;
});

codeInput.addEventListener("compositionend", (event) => {
  isComposingSearch = false;
  handleSearchInputChange(event.target.value);
});

codeInput.addEventListener("input", (event) => {
  if (isComposingSearch) {
    return;
  }
  handleSearchInputChange(event.target.value);
});

codeInput.addEventListener("focus", () => {
  syncSearchClearButton();
  if (latestSuggestions.length > 0 && codeInput.value.trim().length >= minimumSearchLength(codeInput.value)) {
    renderSearchSuggestions(latestSuggestions);
    return;
  }
  scheduleSearchSuggestions(codeInput.value);
});

codeInput.addEventListener("blur", () => {
  window.setTimeout(() => {
    hideSearchSuggestions();
  }, 160);
});

codeInput.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    hideSearchSuggestions();
    return;
  }

  if (event.key === "ArrowDown" || event.key === "ArrowUp") {
    event.preventDefault();
    if (!isSearchSuggestionsOpen()) {
      if (latestSuggestions.length > 0) {
        renderSearchSuggestions(latestSuggestions);
      } else {
        void updateSearchSuggestions(codeInput.value);
        return;
      }
    }
    moveActiveSuggestion(event.key === "ArrowDown" ? 1 : -1);
    return;
  }

  if (event.key === "Enter" && isSearchSuggestionsOpen() && activeSuggestionIndex >= 0 && !isProbableStockCode(codeInput.value)) {
    event.preventDefault();
    commitActiveSuggestion();
  }
});

searchSuggestions.addEventListener("pointerdown", (event) => {
  const button = event.target.closest(".search-suggestion");
  if (!button) {
    return;
  }

  event.preventDefault();
  const index = Number(button.dataset.index);
  const item = latestSuggestions[index];
  if (!item) {
    return;
  }

  applySuggestion(item);
});

searchClearButton.addEventListener("click", () => {
  clearSelectedSuggestion();
  codeInput.value = "";
  hideSearchSuggestions();
  syncSearchClearButton();
  setStatus("待機中", "idle");
  codeInput.focus();
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

function formatChartValue(value, valueType = "plain") {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }

  switch (valueType) {
    case "percent":
      return `${value.toFixed(2)}%`;
    case "ratio-1":
      return value.toFixed(1);
    case "compact-yen":
      return `${formatCompact(value)}円`;
    case "compact":
      return formatCompact(value);
    case "index":
      return value.toFixed(1);
    default:
      return value.toFixed(2);
  }
}

function hasUsableValues(data) {
  return Array.isArray(data) && data.some((value) => value !== null && !Number.isNaN(value));
}

function makeLineDataset(key, data, overrides = {}) {
  if (!hasUsableValues(data)) {
    return null;
  }

  const palette = datasetPalette[key] || {};
  return {
    type: overrides.type || "line",
    label: overrides.label || palette.label || key,
    data,
    borderColor: overrides.borderColor || palette.color || "#1f2740",
    backgroundColor: overrides.backgroundColor || palette.fill || palette.color || "#1f2740",
    tension: overrides.tension ?? 0.34,
    borderWidth: overrides.borderWidth ?? 2.4,
    pointRadius: overrides.pointRadius ?? 0,
    pointHoverRadius: overrides.pointHoverRadius ?? 3.5,
    pointHitRadius: 18,
    spanGaps: true,
    fill: false,
    borderDash: overrides.borderDash || [],
    yAxisID: overrides.yAxisID,
    valueType: overrides.valueType,
  };
}

function makeBarDataset(key, data, overrides = {}) {
  if (!hasUsableValues(data)) {
    return null;
  }

  const palette = datasetPalette[key] || {};
  return {
    type: overrides.type || "bar",
    label: overrides.label || palette.label || key,
    data,
    borderColor: overrides.borderColor || palette.color || "#1f2740",
    backgroundColor: overrides.backgroundColor || palette.fill || palette.color || "#1f2740",
    borderRadius: 12,
    borderSkipped: false,
    borderWidth: 1,
    maxBarThickness: 32,
    yAxisID: overrides.yAxisID,
    valueType: overrides.valueType,
  };
}

function baseOptions({ valueType = "plain", showLegend = true, extraScales = {} } = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: "index",
      intersect: false,
    },
    animation: {
      duration: 500,
      easing: "easeOutQuart",
    },
    plugins: {
      legend: {
        display: showLegend,
        labels: {
          usePointStyle: true,
          boxWidth: 10,
          boxHeight: 10,
          padding: 18,
          color: "#50607d",
          font: {
            family: "Noto Sans JP",
            size: 12,
          },
        },
      },
      tooltip: {
        backgroundColor: "rgba(24, 30, 50, 0.92)",
        titleFont: {
          family: "Manrope",
          size: 13,
          weight: "700",
        },
        bodyFont: {
          family: "Noto Sans JP",
          size: 12,
        },
        padding: 12,
        cornerRadius: 14,
        callbacks: {
          label(context) {
            const currentValueType = context.dataset.valueType || valueType;
            return `${context.dataset.label}: ${formatChartValue(context.parsed.y, currentValueType)}`;
          },
        },
      },
    },
    scales: {
      x: {
        grid: {
          color: "rgba(114, 129, 160, 0.08)",
          drawBorder: false,
        },
        ticks: {
          color: "#72809b",
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 8,
          font: {
            family: "Manrope",
            size: 11,
          },
        },
      },
      y: {
        grid: {
          color: "rgba(114, 129, 160, 0.1)",
          drawBorder: false,
        },
        ticks: {
          color: "#72809b",
          callback(value) {
            if (valueType === "percent") {
              return `${value}%`;
            }
            if (valueType === "ratio-1") {
              return Number(value).toFixed(1);
            }
            if (valueType === "compact-yen") {
              return formatCompact(value);
            }
            if (valueType === "compact") {
              return formatCompact(value);
            }
            return value;
          },
          font: {
            family: "Manrope",
            size: 11,
          },
        },
      },
      ...extraScales,
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

function minimumSearchLength(query) {
  return isProbableStockCode(query) ? 1 : 2;
}

function normalizeSearchText(value) {
  return (value || "").normalize("NFKC").replace(/\s+/g, "").toLocaleLowerCase("ja-JP");
}

function normalizeCodeInput(value) {
  return (value || "").normalize("NFKC").replace(/\s+/g, "").toUpperCase();
}

function isProbableStockCode(value) {
  return /^(?=.*\d)[0-9A-Z]{4,5}$/.test(normalizeCodeInput(value));
}

function isSearchSuggestionsOpen() {
  return !searchSuggestions.classList.contains("hidden") && latestSuggestions.length > 0;
}

function syncSearchClearButton() {
  searchClearButton.classList.toggle("hidden", !codeInput.value.trim());
}

function clearSelectedSuggestion() {
  selectedSuggestion = null;
  codeInput.dataset.resolvedCode = "";
}

function matchesSelectedSuggestionInput(value) {
  if (!selectedSuggestion) {
    return false;
  }

  const normalizedValue = normalizeCodeInput(value);
  return [selectedSuggestion.code, selectedSuggestion.apiCode]
    .filter(Boolean)
    .map((item) => normalizeCodeInput(item))
    .includes(normalizedValue);
}

function handleSearchInputChange(value) {
  if (!matchesSelectedSuggestionInput(value)) {
    clearSelectedSuggestion();
  }
  syncSearchClearButton();
  scheduleSearchSuggestions(value);
}

function syncSuggestionAccessibilityState() {
  codeInput.setAttribute("aria-expanded", isSearchSuggestionsOpen() ? "true" : "false");
  if (isSearchSuggestionsOpen() && activeSuggestionIndex >= 0) {
    codeInput.setAttribute("aria-activedescendant", `search-suggestion-${activeSuggestionIndex}`);
    return;
  }
  codeInput.removeAttribute("aria-activedescendant");
}

function setActiveSuggestion(index) {
  if (!latestSuggestions.length) {
    activeSuggestionIndex = -1;
    syncSuggestionAccessibilityState();
    return;
  }

  const count = latestSuggestions.length;
  activeSuggestionIndex = ((index % count) + count) % count;

  const buttons = searchSuggestions.querySelectorAll(".search-suggestion");
  buttons.forEach((button, buttonIndex) => {
    const isActive = buttonIndex === activeSuggestionIndex;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-selected", isActive ? "true" : "false");
    if (isActive) {
      button.scrollIntoView({ block: "nearest" });
    }
  });
  syncSuggestionAccessibilityState();
}

function moveActiveSuggestion(offset) {
  if (!latestSuggestions.length) {
    return;
  }

  const nextIndex = activeSuggestionIndex < 0 ? (offset > 0 ? 0 : latestSuggestions.length - 1) : activeSuggestionIndex + offset;
  setActiveSuggestion(nextIndex);
}

function commitActiveSuggestion() {
  const item = latestSuggestions[activeSuggestionIndex];
  if (item) {
    applySuggestion(item);
  }
}

function hideSearchSuggestions() {
  searchSuggestions.innerHTML = "";
  searchSuggestions.classList.add("hidden");
  activeSuggestionIndex = -1;
  syncSuggestionAccessibilityState();
}

function renderSearchSuggestions(items) {
  searchSuggestions.innerHTML = "";

  if (!items || items.length === 0) {
    searchSuggestions.classList.add("hidden");
    activeSuggestionIndex = -1;
    syncSuggestionAccessibilityState();
    return;
  }

  const fragment = document.createDocumentFragment();
  items.forEach((item, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.id = `search-suggestion-${index}`;
    button.className = "search-suggestion";
    button.dataset.index = String(index);
    button.setAttribute("role", "option");
    button.setAttribute("aria-selected", "false");

    const code = document.createElement("span");
    code.className = "search-suggestion-code";
    code.textContent = item.code || "N/A";

    const body = document.createElement("span");
    body.className = "search-suggestion-body";

    const name = document.createElement("span");
    name.className = "search-suggestion-name";
    name.textContent = item.name || item.nameEn || "名称未設定";

    const meta = document.createElement("span");
    meta.className = "search-suggestion-meta";
    meta.textContent = [item.market, item.industry].filter(Boolean).join(" / ");

    body.append(name, meta);
    button.append(code, body);
    fragment.appendChild(button);
  });

  searchSuggestions.appendChild(fragment);
  searchSuggestions.classList.remove("hidden");
  setActiveSuggestion(0);
}

function applySuggestion(item) {
  selectedSuggestion = item;
  codeInput.value = item.code || "";
  codeInput.dataset.resolvedCode = item.apiCode || item.code || "";
  syncSearchClearButton();
  hideSearchSuggestions();
  codeInput.focus({ preventScroll: true });
}

function findExactSuggestionMatches(items, query) {
  const normalizedQuery = normalizeSearchText(query);
  if (!normalizedQuery) {
    return [];
  }

  return items.filter((item) =>
    [item.code, item.apiCode, item.name, item.nameEn]
      .filter(Boolean)
      .map((value) => normalizeSearchText(value))
      .includes(normalizedQuery),
  );
}

async function requestSearchSuggestions(query, signal) {
  const response = await fetch(`/api/search?query=${encodeURIComponent(query)}`, { signal });
  const payload = await response.json();

  if (!response.ok) {
    throw new Error(payload.detail || "銘柄候補の取得に失敗しました。");
  }

  return Array.isArray(payload.items) ? payload.items : [];
}

async function updateSearchSuggestions(query) {
  const trimmed = query.trim();
  if (normalizeSearchText(trimmed).length < minimumSearchLength(trimmed)) {
    latestSuggestions = [];
    hideSearchSuggestions();
    return;
  }

  if (searchAbortController) {
    searchAbortController.abort();
  }

  const controller = new AbortController();
  searchAbortController = controller;

  try {
    const items = await requestSearchSuggestions(trimmed, controller.signal);
    if (searchAbortController !== controller) {
      return;
    }

    latestSuggestions = items;
    if (document.activeElement === codeInput) {
      renderSearchSuggestions(items);
    }
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }
    latestSuggestions = [];
    hideSearchSuggestions();
  } finally {
    if (searchAbortController === controller) {
      searchAbortController = null;
    }
  }
}

function scheduleSearchSuggestions(query) {
  window.clearTimeout(searchDebounceTimer);

  const trimmed = query.trim();
  if (normalizeSearchText(trimmed).length < minimumSearchLength(trimmed)) {
    latestSuggestions = [];
    hideSearchSuggestions();
    return;
  }

  searchDebounceTimer = window.setTimeout(() => {
    updateSearchSuggestions(trimmed);
  }, 180);
}

async function resolveAnalyzeCode(rawValue) {
  const trimmed = rawValue.trim();
  if (!trimmed) {
    return null;
  }

  if (matchesSelectedSuggestionInput(trimmed)) {
    return codeInput.dataset.resolvedCode || selectedSuggestion?.apiCode || selectedSuggestion?.code || normalizeCodeInput(trimmed);
  }

  if (isProbableStockCode(trimmed)) {
    return normalizeCodeInput(trimmed);
  }

  const cachedExactMatches = findExactSuggestionMatches(latestSuggestions, trimmed);
  if (cachedExactMatches.length === 1) {
    applySuggestion(cachedExactMatches[0]);
    return cachedExactMatches[0].apiCode || cachedExactMatches[0].code;
  }

  const items = await requestSearchSuggestions(trimmed);
  latestSuggestions = items;
  renderSearchSuggestions(items);

  const exactMatches = findExactSuggestionMatches(items, trimmed);
  if (exactMatches.length === 1) {
    applySuggestion(exactMatches[0]);
    return exactMatches[0].apiCode || exactMatches[0].code;
  }

  if (items.length === 1) {
    applySuggestion(items[0]);
    return items[0].apiCode || items[0].code;
  }

  if (items.length === 0) {
    setStatus("一致する銘柄候補が見つかりません。", "error");
  } else {
    setStatus("候補が複数あります。クリックまたは上下キー + Enter で1件選択してください。", "error");
  }
  return null;
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

function renderValuationChart(canvasId, labels, companySeries, industrySeries, topixSeries, key, valueType = "plain") {
  renderChart(canvasId, {
    type: "line",
    data: {
      labels,
      datasets: [
        makeLineDataset(key, companySeries, { valueType }),
        makeLineDataset("industry", industrySeries, { valueType }),
        makeLineDataset("topix", topixSeries, {
          yAxisID: "yTopix",
          valueType: "index",
          borderDash: [7, 6],
          borderWidth: 2,
        }),
      ].filter(Boolean),
    },
    options: baseOptions({
      valueType,
      extraScales: {
        yTopix: {
          position: "right",
          grid: {
            drawOnChartArea: false,
            drawBorder: false,
          },
          ticks: {
            color: "#d46e5b",
            callback(value) {
              return value;
            },
            font: {
              family: "Manrope",
              size: 11,
            },
          },
        },
      },
    }),
  });
}

function renderEfficiencyChart(canvasId, labels, companySeries, industrySeries, key) {
  renderChart(canvasId, {
    type: "line",
    data: {
      labels,
      datasets: [
        makeLineDataset(key, companySeries, { valueType: "percent" }),
        makeLineDataset("industry", industrySeries, {
          valueType: "percent",
          borderDash: [7, 6],
        }),
      ].filter(Boolean),
    },
    options: baseOptions({ valueType: "percent" }),
  });
}

function renderCharts(chartData) {
  renderChart("weekly-price-chart", {
    type: "line",
    data: {
      labels: chartData.weeklyPrice.labels,
      datasets: [
        makeLineDataset("close", chartData.weeklyPrice.series.close),
        makeLineDataset("ma25", chartData.weeklyPrice.series.ma25),
        makeLineDataset("ma50", chartData.weeklyPrice.series.ma50),
        makeLineDataset("topix", chartData.weeklyPrice.series.topix, {
          yAxisID: "yTopix",
          valueType: "index",
          borderDash: [7, 6],
          borderWidth: 2,
        }),
      ].filter(Boolean),
    },
    options: baseOptions({
      extraScales: {
        yTopix: {
          position: "right",
          grid: {
            drawOnChartArea: false,
            drawBorder: false,
          },
          ticks: {
            color: "#d46e5b",
            font: {
              family: "Manrope",
              size: 11,
            },
          },
        },
      },
    }),
  });

  renderChart("volume-chart", {
    type: "line",
    data: {
      labels: chartData.weeklyVolume.labels,
      datasets: [makeLineDataset("volume", chartData.weeklyVolume.series.volume, { valueType: "compact" })].filter(
        Boolean,
      ),
    },
    options: baseOptions({ valueType: "compact", showLegend: false }),
  });

  renderChart("market-cap-chart", {
    type: "line",
    data: {
      labels: chartData.weeklyMarketCap.labels,
      datasets: [
        makeLineDataset("marketCap", chartData.weeklyMarketCap.series.marketCap, {
          valueType: "compact-yen",
        }),
      ].filter(Boolean),
    },
    options: baseOptions({ valueType: "compact-yen", showLegend: false }),
  });

  renderValuationChart(
    "psr-chart",
    chartData.valuation.labels,
    chartData.valuation.series.psr,
    chartData.valuation.series.psrIndustry,
    chartData.valuation.series.topix,
    "psr",
  );
  renderValuationChart(
    "per-chart",
    chartData.valuation.labels,
    chartData.valuation.series.per,
    chartData.valuation.series.perIndustry,
    chartData.valuation.series.topix,
    "per",
  );
  renderValuationChart(
    "pbr-chart",
    chartData.valuation.labels,
    chartData.valuation.series.pbr,
    chartData.valuation.series.pbrIndustry,
    chartData.valuation.series.topix,
    "pbr",
    "ratio-1",
  );

  renderEfficiencyChart(
    "roe-chart",
    chartData.efficiency.labels,
    chartData.efficiency.series.roe,
    chartData.efficiency.series.roeIndustry,
    "roe",
  );
  renderEfficiencyChart(
    "roa-chart",
    chartData.efficiency.labels,
    chartData.efficiency.series.roa,
    chartData.efficiency.series.roaIndustry,
    "roa",
  );

  renderChart("sales-chart", {
    type: "bar",
    data: {
      labels: chartData.yearEndResults.labels,
      datasets: [makeBarDataset("sales", chartData.yearEndResults.series.sales, { valueType: "compact-yen" })].filter(
        Boolean,
      ),
    },
    options: baseOptions({ valueType: "compact-yen", showLegend: false }),
  });

  renderChart("op-chart", {
    type: "bar",
    data: {
      labels: chartData.yearEndResults.labels,
      datasets: [makeBarDataset("op", chartData.yearEndResults.series.op, { valueType: "compact-yen" })].filter(
        Boolean,
      ),
    },
    options: baseOptions({ valueType: "compact-yen", showLegend: false }),
  });

  renderChart("odp-chart", {
    type: "bar",
    data: {
      labels: chartData.yearEndResults.labels,
      datasets: [makeBarDataset("odp", chartData.yearEndResults.series.odp, { valueType: "compact-yen" })].filter(
        Boolean,
      ),
    },
    options: baseOptions({ valueType: "compact-yen", showLegend: false }),
  });

  renderChart("np-chart", {
    type: "bar",
    data: {
      labels: chartData.yearEndResults.labels,
      datasets: [makeBarDataset("np", chartData.yearEndResults.series.np, { valueType: "compact-yen" })].filter(
        Boolean,
      ),
    },
    options: baseOptions({ valueType: "compact-yen", showLegend: false }),
  });

  renderChart("year-yoy-chart", {
    type: "line",
    data: {
      labels: chartData.yearEndYoy.labels,
      datasets: [
        makeLineDataset("sales", chartData.yearEndYoy.series.sales, { valueType: "percent" }),
        makeLineDataset("op", chartData.yearEndYoy.series.op, { valueType: "percent" }),
        makeLineDataset("odp", chartData.yearEndYoy.series.odp, { valueType: "percent" }),
        makeLineDataset("np", chartData.yearEndYoy.series.np, { valueType: "percent" }),
      ].filter(Boolean),
    },
    options: baseOptions({ valueType: "percent" }),
  });

  renderChart("quarterly-yoy-chart", {
    type: "line",
    data: {
      labels: chartData.quarterlyYoy.labels,
      datasets: [
        makeLineDataset("sales", chartData.quarterlyYoy.series.sales, { valueType: "percent" }),
        makeLineDataset("op", chartData.quarterlyYoy.series.op, { valueType: "percent" }),
        makeLineDataset("odp", chartData.quarterlyYoy.series.odp, { valueType: "percent" }),
        makeLineDataset("np", chartData.quarterlyYoy.series.np, { valueType: "percent" }),
      ].filter(Boolean),
    },
    options: baseOptions({ valueType: "percent" }),
  });

  chartsGrid.classList.remove("hidden");
}

async function analyze() {
  const rawInput = codeInput.value.trim();
  if (!rawInput) {
    setStatus("銘柄コードまたは銘柄名を入力してください。", "error");
    return;
  }

  const code = await resolveAnalyzeCode(rawInput);
  if (!code) {
    return;
  }

  hideSearchSuggestions();
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
