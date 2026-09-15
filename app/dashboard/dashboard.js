const state = { listings: [], runs: [] };

const escapeHtml = (value) => String(value ?? "")
  .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;").replaceAll("'", "&#039;");
const formatDate = (value) => value
  ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value))
  : "Not scheduled";
const formatAge = (seconds) => {
  if (seconds == null) return "No successful run recorded";
  const hours = Math.floor(seconds / 3600);
  if (hours < 1) return "Less than an hour ago";
  if (hours < 24) return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  const days = Math.floor(hours / 24);
  return `${days} day${days === 1 ? "" : "s"} ago`;
};
const money = (value) => value == null ? "—" : new Intl.NumberFormat(undefined, {
  style: "currency", currency: "USD", maximumFractionDigits: 0,
}).format(value);
const number = (value, digits = 0) => value == null ? "—" : Number(value).toFixed(digits);
const percent = (value) => value == null ? "—" : `${(Number(value) * 100).toFixed(1)}%`;
const label = (value) => value
  ? String(value).replaceAll("_", " ").replace(/\b\w/g, (character) => character.toUpperCase())
  : "Unavailable";
const dealScore = (row) => row.deal_score_total ?? row.normalized_available_score;

async function loadHealth() {
  const liveState = document.querySelector("#live-state");
  try {
    const response = await fetch("/health", { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error("Health request failed");
    const data = await response.json();
    const shadow = data.shadow_pipeline;
    liveState.className = `live-state ${data.status === "ok" ? "ok" : "error"}`;
    liveState.lastChild.textContent = data.status === "ok" ? " Live data" : " Needs attention";
    document.querySelector("#health-value").textContent = data.status === "ok" ? "Healthy" : "Degraded";
    document.querySelector("#health-detail").textContent = `Shadow data is ${shadow.freshness}`;
    document.querySelector("#last-run-value").textContent = formatDate(shadow.last_successful_run_at);
    document.querySelector("#last-run-detail").textContent = formatAge(shadow.last_successful_run_age_seconds);
    document.querySelector("#next-run-value").textContent = formatDate(shadow.next_run_at);
    document.querySelector("#failure-value").textContent = shadow.recent_failure_count;
  } catch (error) {
    liveState.className = "live-state error";
    liveState.lastChild.textContent = " Offline";
    document.querySelector("#health-value").textContent = "Unavailable";
    document.querySelector("#health-detail").textContent = "Could not reach the local API";
  }
}

function populateFilters() {
  const cities = [...new Set(state.listings.map((row) => row.city).filter(Boolean))].sort();
  document.querySelector("#city-filter").insertAdjacentHTML(
    "beforeend",
    cities.map((city) => `<option value="${escapeHtml(city)}">${escapeHtml(city)}</option>`).join(""),
  );
}

function renderSummary(data) {
  const summary = data.summary;
  document.querySelector("#listing-count").textContent = summary.listing_count;
  document.querySelector("#score-count").textContent = summary.complete_score_count;
  document.querySelector("#review-count").textContent = summary.review_count;
  document.querySelector("#missing-count").textContent = summary.missing_sqft_count;
  document.querySelector("#observation-days").textContent = data.readiness.observation_days;
  document.querySelector("#valuation-coverage").textContent = `${(data.readiness.minimum_supported_valuation_coverage * 100).toFixed(1)}%`;
  const checks = data.readiness.checks;
  const passed = Object.values(checks).filter(Boolean).length;
  document.querySelector("#readiness-title").textContent = data.readiness.ready
    ? "Ready for final review" : "Observation in progress";
  document.querySelector("#readiness-copy").textContent = `${passed} of ${Object.keys(checks).length} automated checks pass. Comparable sales remain provisional.`;
}

function filteredListings() {
  const query = document.querySelector("#search-input").value.trim().toLowerCase();
  const city = document.querySelector("#city-filter").value;
  const confidence = document.querySelector("#confidence-filter").value;
  const reviewOnly = document.querySelector("#review-filter").checked;
  const sort = document.querySelector("#sort-select").value;
  const rows = state.listings.filter((row) => {
    const haystack = `${row.address ?? ""} ${row.city ?? ""} ${row.source_listing_id}`.toLowerCase();
    const actualConfidence = row.confidence_label ?? "unavailable";
    return (!query || haystack.includes(query)) && (!city || row.city === city)
      && (!confidence || actualConfidence === confidence)
      && (!reviewOnly || row.review_flags.length > 0);
  });
  const descending = (key) => (left, right) => (right[key] ?? -Infinity) - (left[key] ?? -Infinity);
  if (sort === "discount") rows.sort(descending("valuation_discount_pct"));
  else if (sort === "price") rows.sort((left, right) => (left.list_price ?? Infinity) - (right.list_price ?? Infinity));
  else if (sort === "dom") rows.sort(descending("days_on_market"));
  else rows.sort((left, right) => (dealScore(right) ?? -Infinity) - (dealScore(left) ?? -Infinity));
  return rows;
}

function renderListings() {
  const rows = filteredListings();
  const body = document.querySelector("#listing-rows");
  document.querySelector("#result-count").textContent = `${rows.length} of ${state.listings.length} listings`;
  document.querySelector("#empty-state").hidden = rows.length > 0;
  body.innerHTML = rows.map((row) => {
    const index = state.listings.indexOf(row);
    const score = dealScore(row);
    const review = row.review_flags.length
      ? `<span class="badge review">${row.review_flags.length} flag${row.review_flags.length === 1 ? "" : "s"}</span>`
      : `<span class="badge high">Clear</span>`;
    return `<tr tabindex="0" data-index="${index}" aria-label="Open ${escapeHtml(row.address)} details">
      <td class="property-cell"><strong>${escapeHtml(row.address || "Address unavailable")}</strong><span>${escapeHtml(row.city)}, ${escapeHtml(row.state)} · ${escapeHtml(row.property_type || "Unknown type")}</span></td>
      <td data-label="Price">${money(row.list_price)}</td>
      <td data-label="Deal score"><span class="${score == null ? "muted-value" : "score-value"}">${number(score, 1)}</span></td>
      <td data-label="Est. discount">${percent(row.valuation_discount_pct)}</td>
      <td data-label="Confidence"><span class="badge ${escapeHtml(row.confidence_label || "")}">${escapeHtml(label(row.confidence_label))}</span></td>
      <td data-label="Days listed">${row.days_on_market ?? "—"}</td>
      <td data-label="Review">${review}</td>
    </tr>`;
  }).join("");
}

function renderRuns() {
  const container = document.querySelector("#run-list");
  container.innerHTML = state.runs.slice(0, 4).map((run) => {
    const coverage = run.comparable_ready_count
      ? `${((run.supported_valuation_count / run.comparable_ready_count) * 100).toFixed(1)}% valued`
      : "No comparable-ready homes";
    return `<article class="run-item"><strong>Run ${run.id} · ${escapeHtml(label(run.status))}</strong><span>${escapeHtml(formatDate(run.finished_at || run.started_at))}</span><span>${run.listing_count ?? 0} listings · ${coverage}</span></article>`;
  }).join("") || `<p class="empty-state">No shadow runs recorded.</p>`;
}

function openDetail(row) {
  const components = row.components.map((component) => {
    const ratio = component.points == null ? 0
      : Math.max(0, Math.min(100, component.points / component.max_points * 100));
    return `<div class="component"><div class="component-heading"><strong>${escapeHtml(label(component.component_key))}</strong><span>${component.points == null ? "Unavailable" : `${number(component.points, 1)} / ${number(component.max_points, 0)}`}</span></div><div class="component-bar"><span style="width:${ratio}%"></span></div><small>${escapeHtml(component.reason)}</small></div>`;
  }).join("") || `<p class="muted-value">No component score is available.</p>`;
  const flags = row.review_flags.length
    ? `<div class="flag-list"><strong>Review before relying on this score</strong><ul>${row.review_flags.map((flag) => `<li>${escapeHtml(flag)}</li>`).join("")}</ul></div>` : "";
  document.querySelector("#detail-content").innerHTML = `<header class="detail-header"><p class="eyebrow">${escapeHtml(row.source_listing_id)}</p><h2 id="detail-title">${escapeHtml(row.address || "Address unavailable")}</h2><p>${escapeHtml(row.city)}, ${escapeHtml(row.state)} ${escapeHtml(row.zip)} · ${escapeHtml(label(row.property_type))}</p></header>
    ${flags}<div class="detail-stats"><div><span>List price</span><strong>${money(row.list_price)}</strong></div><div><span>Deal score</span><strong>${number(dealScore(row), 1)}</strong></div><div><span>Estimated value</span><strong>${money(row.estimated_value)}</strong></div><div><span>Value range</span><strong>${money(row.estimated_value_low)}–${money(row.estimated_value_high)}</strong></div><div><span>Comparable confidence</span><strong>${escapeHtml(label(row.confidence_label))} ${row.confidence_score == null ? "" : `(${number(row.confidence_score, 1)})`}</strong></div><div><span>Comparable set</span><strong>${row.included_comparable_count ?? 0} of ${row.available_comparable_count ?? 0}</strong></div><div><span>Home details</span><strong>${row.beds ?? "—"} bd · ${row.baths ?? "—"} ba · ${row.sqft ? `${Number(row.sqft).toLocaleString()} ft²` : "area missing"}</strong></div><div><span>Days listed</span><strong>${row.days_on_market ?? "—"}</strong></div></div>
    <section class="component-list"><p class="eyebrow">Score components</p>${components}</section>`;
  document.querySelector("#drawer-backdrop").hidden = false;
  const drawer = document.querySelector("#detail-drawer");
  drawer.classList.add("open");
  drawer.setAttribute("aria-hidden", "false");
  document.querySelector("#drawer-close").focus();
}

function closeDetail() {
  const drawer = document.querySelector("#detail-drawer");
  drawer.classList.remove("open");
  drawer.setAttribute("aria-hidden", "true");
  document.querySelector("#drawer-backdrop").hidden = true;
}

async function loadDashboard() {
  try {
    const response = await fetch("/dashboard/data", { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error("Dashboard request failed");
    const data = await response.json();
    state.listings = data.listings;
    state.runs = data.runs;
    renderSummary(data);
    populateFilters();
    renderListings();
    renderRuns();
  } catch (error) {
    document.querySelector("#result-count").textContent = "Results unavailable";
    document.querySelector("#listing-rows").innerHTML = `<tr><td colspan="7">Could not load listing results. Check the local API and database.</td></tr>`;
  }
}

document.querySelector("#search-input").addEventListener("input", renderListings);
document.querySelectorAll("#city-filter, #confidence-filter, #sort-select, #review-filter")
  .forEach((control) => control.addEventListener("change", renderListings));
document.querySelector("#listing-rows").addEventListener("click", (event) => {
  const row = event.target.closest("tr[data-index]");
  if (row) openDetail(state.listings[Number(row.dataset.index)]);
});
document.querySelector("#listing-rows").addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    const row = event.target.closest("tr[data-index]");
    if (row) {
      event.preventDefault();
      openDetail(state.listings[Number(row.dataset.index)]);
    }
  }
});
document.querySelector("#drawer-close").addEventListener("click", closeDetail);
document.querySelector("#drawer-backdrop").addEventListener("click", closeDetail);
document.addEventListener("keydown", (event) => { if (event.key === "Escape") closeDetail(); });

loadHealth();
loadDashboard();
