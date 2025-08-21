import subprocess
import os
import shutil
import webbrowser
from pathlib import Path
import time

# === Paths ===
BASE_DIR = Path("C:/Users/anish/PycharmProjects/CRM_Dashboard")
REACT_DIR = BASE_DIR / "crm_dashboard"

JSON_SOURCE = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/finalCleanOutput/crm_sirix_enriched.json")
JSON_DEST = REACT_DIR / "public" / "crm_sirix_enriched.json"

# === Ensure React App Exists ===
if not REACT_DIR.exists():
    print("[OK] Creating new React app...")
    subprocess.run(["npx", "create-react-app", "crm_dashboard"], cwd=BASE_DIR, shell=True)

# === Copy JSON ===
print("[OK] Copying JSON data...")
os.makedirs(REACT_DIR / "public", exist_ok=True)
shutil.copy(JSON_SOURCE, JSON_DEST)

# === Writing App.js ===
APP_JS = REACT_DIR / "src" / "App.js"
APP_JS.write_text("""\
import React, { useEffect, useState } from "react";
import "./App.css";

// === Font Face for Aptos Display (local) ===
const addAptosFont = () => {
  const style = document.createElement("style");
  style.innerHTML = `
    @font-face {
      font-family: 'Aptos Display';
      src: url('/fonts/AptosDisplay.ttf') format('truetype');
      font-weight: normal;
      font-style: normal;
    }
  `;
  document.head.appendChild(style);
};
addAptosFont();

// Helpers
function fmtNumber(v, digits = 2) {
  if (v === null || v === undefined || v === "") return "";
  const n = Number(v);
  if (Number.isNaN(n)) return String(v);
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}
function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "";
  const n = Number(v);
  const sign = n > 0 ? "+" : (n < 0 ? "" : "");
  return sign + n.toFixed(2) + "%";
}
function numVal(v) {
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}
function getFlagOnly(countryName) {
  const countryMap = {
    "Albania": "al","Australia": "au","Bahrain": "bh","Bangladesh": "bd","Benin": "bj",
    "Botswana": "bw","Burkina Faso": "bf","Burundi": "bi","Cameroon": "cm","Canada": "ca",
    "Colombia": "co","Cote D'Ivoire": "ci","Cyprus": "cy","Egypt": "eg","Ethiopia": "et",
    "France": "fr","Germany": "de","Ghana": "gh","India": "in","Ireland": "ie",
    "Israel": "il","Jordan": "jo","Kenya": "ke","Lesotho": "ls","Malaysia": "my",
    "Malta": "mt","Nepal": "np","Netherlands": "nl","Nigeria": "ng","Pakistan": "pk",
    "Saudi Arabia": "sa","Senegal": "sn","Singapore": "sg","Somalia": "so",
    "South Africa": "za","Spain": "es","Swaziland": "sz","Tanzania": "tz","Uganda": "ug",
    "United Arab Emirates": "ae","United Kingdom": "gb","Uzbekistan": "uz","Zambia": "zm",
    "Zimbabwe": "zw"
  };
  const code = countryMap[countryName];
  if (!code) return "";
  return (
    <img
      src={`https://flagcdn.com/w40/${code}.png`}
      title={countryName || ""}
      alt={countryName || ""}
      style={{ width: "20px", height: "14px", objectFit: "cover", borderRadius: "2px" }}
    />
  );
}
function shortName(full) {
  if (!full) return "";
  const parts = String(full).trim().split(/\\s+/).filter(Boolean);
  if (parts.length === 0) return "";
  if (parts.length === 1) return parts[0];
  const first = parts[0];
  const last = parts[parts.length - 1];
  return first + " " + (last[0] ? (last[0].toUpperCase() + ".") : "");
}

// Subtle row highlight (background) for top-3
const rowStyleForRank = (rank) => {
  if (rank === 0) return { background: "#fff9ec" }; // soft gold
  if (rank === 1) return { background: "#f5f7ff" }; // soft silver/blue
  if (rank === 2) return { background: "#f7fff5" }; // soft bronze/green
  return {};
};

// Accent stripe color for top-3
const accentForRank = (rank) => {
  if (rank === 0) return "#F4C430"; // gold
  if (rank === 1) return "#B0B7C3"; // silver
  if (rank === 2) return "#CD7F32"; // bronze
  return "transparent";
};

// Medal badge for top-3
const rankBadge = (rank) => {
  if (rank === 0) return <span style={{ fontWeight: 800, fontSize: "15px" }}>🥇</span>;
  if (rank === 1) return <span style={{ fontWeight: 800, fontSize: "15px" }}>🥈</span>;
  if (rank === 2) return <span style={{ fontWeight: 800, fontSize: "15px" }}>🥉</span>;
  return null;
};

function App() {
  const [originalData, setOriginalData] = useState([]);
  const [data, setData] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 100;

  // Load JSON and default sort by NET % DESC
  useEffect(() => {
  fetch("/crm_sirix_enriched.json")
    .then((res) => {
      if (!res.ok) throw new Error("Failed to load JSON");
      return res.json();
    })
    .then((json) => {
      // Exclude rows with Capital ($) = 50,000
      const filtered = json.filter(row => Number(row["Plan"]) !== 50000);

      // Default sort by NET % (PctChange) descending
      const sorted = [...filtered].sort((a, b) => {
        const A = numVal(a["PctChange"]);
        const B = numVal(b["PctChange"]);
        if (A === null && B === null) return 0;
        if (A === null) return 1;
        if (B === null) return -1;
        return B - A;
      });
      setOriginalData(sorted);
      setData(sorted);
    })
    .catch((err) => {
      console.error(err);
      setOriginalData([]);
      setData([]);
    });
}, []);

  // Filter + keep % sort DESC so ranks reflect current filtered order
  const handleSearch = (e) => {
    const query = e.target.value.toLowerCase();
    setSearchQuery(query);
    const filtered = originalData
      .filter((row) => Object.values(row).some((val) => String(val ?? "").toLowerCase().includes(query)))
      .sort((a, b) => {
        const A = numVal(a["PctChange"]);
        const B = numVal(b["PctChange"]);
        if (A === null && B === null) return 0;
        if (A === null) return 1;
        if (B === null) return -1;
        return B - A;
      });
    setData(filtered);
    setCurrentPage(1);
  };

  const totalPages = Math.max(1, Math.ceil(data.length / rowsPerPage));
  const startIndex = (currentPage - 1) * rowsPerPage;
  const paginatedData = data.slice(startIndex, startIndex + rowsPerPage);

  const goToPage = (page) => {
    if (page >= 1 && page <= totalPages) setCurrentPage(page);
  };

  // Center the content to avoid edge-to-edge width
  const centerWrap = { maxWidth: 1100, margin: "0 auto" };

  return (
    <div style={{ padding: "20px", fontFamily: "'Segoe UI', sans-serif", background: "#fafafa" }}>
      <h1
        style={{
          fontSize: "3.2rem",
          fontWeight: "800",
          marginBottom: "10px",
          fontFamily: "'Aptos Display', 'Segoe UI', sans-serif",
          color: "#1a1a1a",
          letterSpacing: "0.7px",
          textAlign: "center",
          textTransform: "uppercase",
          lineHeight: "1.2"
        }}
      >
        CRM Dashboard
      </h1>

      <div style={{ ...centerWrap }}>
        <p style={{ fontSize: "1rem", marginBottom: "20px", textAlign: "center", color: "#666" }}>
          Total Records: {data.length}
        </p>

        <div style={{ marginBottom: "20px", display: "flex", gap: "12px", alignItems: "center", justifyContent: "center" }}>
          <input
            type="text"
            placeholder="Search..."
            value={searchQuery}
            onChange={handleSearch}
            style={{
              padding: "10px 14px",
              width: "260px",
              border: "1px solid #ccc",
              borderRadius: "6px",
              fontSize: "14px",
              fontFamily: "'Segoe UI', sans-serif",
              boxShadow: "1px 1px 5px rgba(0,0,0,0.05)",
              outline: "none"
            }}
          />
        </div>

        <div style={{ overflowX: "auto", maxHeight: "70vh", overflowY: "auto" }}>
          <table
            border="1"
            cellPadding="5"
            style={{
              width: "100%",
              borderCollapse: "collapse",
              textAlign: "center",
              fontFamily: "'Aptos Display', 'Segoe UI', sans-serif",
              fontSize: "14px",
              background: "#fff"
            }}
          >
            <thead>
              <tr style={{ background: "#f0f0f0" }}>
                {["RANK", "NAME", "NET %", "CAPITAL ($)", "COUNTRY"].map((label, idx) => (
                  <th key={idx} style={{ fontWeight: 1000, fontSize: "20px", padding: "10px 6px", whiteSpace: "nowrap" }}>
                    {label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {paginatedData.length === 0 ? (
                <tr>
                  <td colSpan={5} style={{ padding: 20, color: "#777" }}>
                    No records found.
                  </td>
                </tr>
              ) : (
                paginatedData.map((row, rowIndex) => {
                  const globalRank = startIndex + rowIndex; // 0-based rank in current filtered+sorted data

                  // Zebra + top-3 highlight
                  const zebra = { background: rowIndex % 2 === 0 ? "#ffffff" : "#f9f9f9" };
                  const highlight = rowStyleForRank(globalRank);
                  const rowStyle = { ...zebra, ...highlight };

                  // Full-row font scaling for top 3
                  let rowFontSize = "14px";
                  let rowFontWeight = 400;
                  if (globalRank === 0) { rowFontSize = "17px"; rowFontWeight = 800; }  // 1st
                  else if (globalRank === 1) { rowFontSize = "16px"; rowFontWeight = 700; } // 2nd
                  else if (globalRank === 2) { rowFontSize = "15px"; rowFontWeight = 600; } // 3rd

                  // Left accent stripe for top-3
                  const leftAccent = accentForRank(globalRank);

                  // NET % color + extra emphasis
                  const n = numVal(row["PctChange"]);
                  const pctColor = n == null ? "#222" : (n > 0 ? "#1e8e3e" : (n < 0 ? "#d93025" : "#222"));
                  let pctFont = rowFontSize;
                  if (globalRank === 0) pctFont = "calc(17px + 6px)";
                  else if (globalRank === 1) pctFont = "calc(16px + 4px)";
                  else if (globalRank === 2) pctFont = "calc(15px + 2px)";

                  // Common cell style for each row (inherits row size/weight)
                  const cellBase = { whiteSpace: "nowrap", fontSize: rowFontSize, fontWeight: rowFontWeight };

                  return (
                    <tr key={rowIndex} style={rowStyle}>
                      {/* RANK with medal + left stripe */}
                      <td
                        style={{
                          ...cellBase,
                          fontWeight: 800,
                          borderLeft: `8px solid ${leftAccent}`,
                        }}
                      >
                        <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                          {rankBadge(globalRank)}
                          {globalRank + 1}
                        </span>
                      </td>

                      {/* NAME (first name + initial) */}
                      <td style={cellBase}>{shortName(row["CUSTOMER NAME"])}</td>

                      {/* NET % */}
                      <td style={cellBase}>
                        <span style={{ color: pctColor, fontWeight: 800, fontSize: pctFont }}>
                          {fmtPct(n)}
                        </span>
                      </td>

                      {/* CAPITAL ($) from Plan */}
                      <td style={cellBase}>{fmtNumber(row["Plan"], 0)}</td>

                      {/* COUNTRY (flag only) */}
                      <td style={cellBase}>{getFlagOnly(row["Country"])}</td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <div style={{ marginTop: "20px", display: "flex", justifyContent: "center", gap: 12 }}>
          <button onClick={() => goToPage(currentPage - 1)} disabled={currentPage === 1} style={{ padding: "6px 10px" }}>
            Prev
          </button>
          <span style={{ margin: "0 10px" }}>
            Page {currentPage} of {totalPages}
          </span>
          <button onClick={() => goToPage(currentPage + 1)} disabled={currentPage === totalPages} style={{ padding: "6px 10px" }}>
            Next
          </button>
        </div>
      </div>
    </div>
  );
}

export default App;
""", encoding="utf-8")

# === Start React App on a custom port (avoid clashes) ===
print("[OK] Starting React server on PORT 3001...")
subprocess.Popen(
    ["C:/Program Files/nodejs/npm.cmd", "start"],
    cwd=REACT_DIR,
    shell=True,
    env={**os.environ, "PORT": "3001"}
)

# === Open Browser ===
time.sleep(5)
webbrowser.open("http://localhost:3001")
