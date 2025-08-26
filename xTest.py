import subprocess
import os
import shutil
import webbrowser
from pathlib import Path
import time

# === Paths ===
BASE_DIR = Path("C:/Users/anish/PycharmProjects/CRM_Dashboard")
REACT_DIR = BASE_DIR / "crm_dashboard"

JSON_SOURCE = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/finalCleanOutput/crm_sirix_enrichedNEW.json")
JSON_DEST = REACT_DIR / "public" / "crm_sirix_enrichedNEW.json"

# === Ensure React App Exists ===
if not REACT_DIR.exists():
    print("[OK] Creating new React app...")
    subprocess.run(["npx", "create-react-app", "crm_dashboard"], cwd=BASE_DIR, shell=True)

# === Copy JSON ===
print("[OK] Copying JSON data...")
os.makedirs(REACT_DIR / "public", exist_ok=True)
shutil.copy(JSON_SOURCE, JSON_DEST)

# === Write App.js ===
APP_JS = REACT_DIR / "src" / "App.js"
APP_JS.write_text("""\
import React, { useEffect, useState } from "react";
import "./App.css";

// === Font + Animations (injected) ===
const addAptosFont = () => {
  const style = document.createElement("style");
  style.innerHTML = `
    @font-face {
      font-family: 'Aptos Display';
      src: url('/fonts/AptosDisplay.ttf') format('truetype');
      font-weight: normal;
      font-style: normal;
    }
    @keyframes gradientShift {
      0% { background-position: 0% 50%; opacity: 0.92; }
      50% { background-position: 100% 50%; opacity: 1; }
      100% { background-position: 0% 50%; opacity: 0.92; }
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
function pad2(n){ return String(n).padStart(2, "0"); }

// === Countdown helpers ===
function getThisMondayNoon(d = new Date()) {
  const day = d.getDay();               // 0=Sun,1=Mon
  const diffToMonday = (day + 6) % 7;   // days since Monday
  const monday = new Date(d);
  monday.setDate(d.getDate() - diffToMonday);
  monday.setHours(12, 0, 0, 0);         // 12:00 local
  return monday;
}
function getNextResetTarget(now = new Date()) {
  const thisMondayNoon = getThisMondayNoon(now);
  if (now < thisMondayNoon) return thisMondayNoon; // this week's Monday 12:00
  const next = new Date(thisMondayNoon);
  next.setDate(thisMondayNoon.getDate() + 7);      // next Monday 12:00
  return next;
}
function diffToDHMS(target, now = new Date()) {
  let ms = Math.max(0, target - now);
  const totalSec = Math.floor(ms / 1000);
  const d = Math.floor(totalSec / 86400);
  const h = Math.floor((totalSec % 86400) / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  return { d, h, m, s };
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
      style={{ 
        width: "38px",
        height: "28px",
        objectFit: "cover",
        borderRadius: "3px",
        boxShadow: "0 0 3px rgba(1,2,2,5)"
      }}
    />
  );
}

function shortName(full) {
  if (!full) return "";
  const parts = String(full).trim().split(/\\s+/).filter(Boolean);
  if (parts.length === 0) return "";
  const capWord = (s) => s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
  const first = capWord(parts[0]);                 // Manny
  const last = parts[parts.length - 1] || "";      // H.
  const lastInitial = last ? last[0].toUpperCase() + "." : "";
  return lastInitial ? `${first} ${lastInitial}` : first;
}

// Top-3 row styles (by GLOBAL rank)
const rowStyleForRank = (r) => {
  if (r === 0) return { background: "#fff9ec" };
  if (r === 1) return { background: "#f5f7ff" };
  if (r === 2) return { background: "#f7fff5" };
  return {};
};
const rowHeightForRank = (r) => {
  if (r === 0) return 45; // top 1
  if (r === 1) return 43; // #2
  if (r === 2) return 41; // #3
  return 42;              // everyone else
};
const accentForRank = (r) => {
  if (r === 0) return "#F4C430";
  if (r === 1) return "#B0B7C3";
  if (r === 2) return "#CD7F32";
  return "transparent";
};
const rankBadge = (r) => {
  if (r === 0) return <span style={{ fontWeight: 800, fontSize: "20px" }}>🥇</span>;
  if (r === 1) return <span style={{ fontWeight: 800, fontSize: "20px" }}>🥈</span>;
  if (r === 2) return <span style={{ fontWeight: 800, fontSize: "20px" }}>🥉</span>;
  return null;
};

function App() {
  const [originalData, setOriginalData] = useState([]);
  const [data, setData] = useState([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 100;

  // Countdown state
  const [target, setTarget] = useState(getNextResetTarget());
  const [tleft, setTleft] = useState(diffToDHMS(target));

  // Prize map for GLOBAL ranks (1..10)
  const prizeMap = {
    1: "$10,000 Funded Account",
    2: "$5,000 Funded Account",
    3: "$2,500 Funded Account",
    4: "$1,000 Instant Funded Account",
    5: "$1,000 Instant Funded Account",
    6: "$1,000 Instant Funded Account",
    7: "$1,000 Instant Funded Account",
    8: "$1,000 Instant Funded Account",
    9: "$1,000 Instant Funded Account",
    10: "$1,000 Instant Funded Account",
  };

  // --------- NEW: data loader + scheduler ---------
  // Reusable loader (with cache-busting)
  async function loadData() {
    try {
      const res = await fetch(`/crm_sirix_enrichedNEW.json?ts=${Date.now()}`);
      if (!res.ok) throw new Error("Failed to load JSON");
      const json = await res.json();
      const sorted = [...json].sort((a, b) => {
        const A = numVal(a["PctChange"]);
        const B = numVal(b["PctChange"]);
        if (A === null && B === null) return 0;
        if (A === null) return 1;
        if (B === null) return -1;
        return B - A;
      });
      setOriginalData(json);
      setData(sorted);
    } catch (e) {
      setOriginalData([]);
      setData([]);
    }
  }

  // compute ms until next 00:30/04:30/08:30/12:30/16:30/20:30
  function msUntilNextScheduledHalfHour() {
    const now = new Date();
    const scheduleHours = [0, 4, 8, 12, 16, 20];
    for (let i = 0; i < scheduleHours.length; i++) {
      const h = scheduleHours[i];
      const t = new Date(now);
      t.setHours(h, 30, 0, 0); // h:30:00.000
      if (t > now) return t - now;
    }
    // none left today -> next day 00:30
    const nxt = new Date(now);
    nxt.setDate(now.getDate() + 1);
    nxt.setHours(0, 30, 0, 0);
    return nxt - now;
  }

  // Load once on mount
  useEffect(() => {
    loadData();
  }, []);

  // Schedule refresh exactly at those times
  useEffect(() => {
    let cancelled = false;
    let timeoutId;

    async function arm() {
      const ms = msUntilNextScheduledHalfHour();
      timeoutId = setTimeout(async () => {
        if (cancelled) return;
        await loadData();
        arm(); // arm again for the following slot
      }, ms);
    }

    arm();
    return () => { cancelled = true; if (timeoutId) clearTimeout(timeoutId); };
  }, []);
  // --------- /NEW ---------

  // Live countdown tick
  useEffect(() => {
    const id = setInterval(() => {
      const now = new Date();
      if (now >= target) {
        const nextT = getNextResetTarget(now);
        setTarget(nextT);
        setTleft(diffToDHMS(nextT, now));
      } else {
        setTleft(diffToDHMS(target, now));
      }
    }, 1000);
    return () => clearInterval(id);
  }, [target]);

  // Search (keeps sort by % desc)
  const handleSearch = (e) => {
    const q = e.target.value.toLowerCase();
    setSearchQuery(q);
    const filtered = originalData
      .filter(row => Object.values(row).some(val => String(val ?? "").toLowerCase().includes(q)))
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

  // Paging
  const totalPages = Math.max(1, Math.ceil(data.length / rowsPerPage));
  const startIndex = (currentPage - 1) * rowsPerPage;
  const paginatedData = data.slice(startIndex, startIndex + rowsPerPage);
  const goToPage = (page) => setCurrentPage(Math.max(1, Math.min(totalPages, page)));

  // Layout helpers (make center a bit wider by increasing maxWidth)
  const centerWrap = { maxWidth: 1300, margin: "0 auto" };
  const gradientTheadStyle = {
    background: "linear-gradient(135deg, #0f0f0f 0%, #222 60%, #d4af37 100%)",
    color: "#fff"
  };

  // PRIZES shows first 10 rows of the CURRENT PAGE (aligns heights)
  const visibleForPrizes = paginatedData.slice(0, 10);

  return (
    <div style={{ padding: "20px", fontFamily: "'Segoe UI', sans-serif", background: "#fafafa" }}>
      {/* Title */}
      <h1
        style={{
          fontSize: "3.0rem",
          fontWeight: "900",
          marginBottom: "16px",
          fontFamily: "'Aptos Display', 'Segoe UI', sans-serif",
          letterSpacing: "0.7px",
          textAlign: "center",
          textTransform: "uppercase",
          lineHeight: "1.15",
          background: "linear-gradient(90deg, #111 0%, #d4af37 25%, #111 50%, #d4af37 75%, #111 100%)",
          backgroundSize: "300% 100%",
          WebkitBackgroundClip: "text",
          backgroundClip: "text",
          color: "transparent",
          animation: "gradientShift 6s ease-in-out infinite"
        }}
      >
        E2T WORLD CUP COMPETITION
      </h1>

      {/* Shared top bar (ABOVE TABLES) */}
      <div style={{ ...centerWrap }}>
        <div style={{ marginBottom: "16px", display: "flex", gap: "12px", alignItems: "center", justifyContent: "center" }}>
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
              boxShadow: "1px 1px 5px rgba(0,0,0,2)",
              outline: "none"
            }}
          />
        </div>
      </div>

      {/* PRIZES (left) + LEADERBOARD (center) + COUNTDOWN (right) */}
      <div style={{ display: "flex", gap: 18, alignItems: "flex-start", ...centerWrap }}>
        {/* PRIZES table */}
        <div style={{ flex: "0 0 260px" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 13,
              background: "#fff",
              boxShadow: "0 1px 6px rgba(0,0,0,2)",
              borderRadius: 8,
              overflow: "hidden"
            }}
          >
            <thead style={gradientTheadStyle}>
              <tr>
                <th style={{ padding: "10px 8px", fontWeight: 900, textAlign: "left", fontSize: 14 }}>PRIZES</th>
                <th style={{ padding: "10px 8px", fontWeight: 900, textAlign: "right", fontSize: 14 }}>Amount</th>
              </tr>
            </thead>
            <tbody>
              {visibleForPrizes.length === 0 && (
                <tr><td colSpan={2} style={{ padding: 10, color: "#777" }}>No data</td></tr>
              )}
              {visibleForPrizes.map((row, idx) => {
                const globalRank = startIndex + idx; // 0-based global
                const zebra = { background: idx % 2 === 0 ? "#ffffff" : "#fafafa" };
                const highlight = rowStyleForRank(globalRank);
                const rowStyle = { ...zebra, ...highlight };
                const prize = prizeMap[globalRank + 1] || "";

                // fixed row height to match leaderboard
                const rh = rowHeightForRank(globalRank);

                // optional slight font boost for top-3
                let fs = "13px", fw = 500;
                if (globalRank === 0) { fs = "15px"; fw = 800; }
                else if (globalRank === 1) { fs = "14px"; fw = 700; }
                else if (globalRank === 2) { fs = "13.5px"; fw = 600; }

                return (
                  <tr key={idx} style={rowStyle}>
                    <td style={{
                      height: rh,
                      lineHeight: rh + "px",
                      padding: 0,
                      fontWeight: 800,
                      borderLeft: `6px solid ${accentForRank(globalRank)}`
                    }}>
                      {rankBadge(globalRank) || (globalRank + 1)}
                    </td>
                    <td style={{
                      height: rh,
                      lineHeight: rh + "px",
                      padding: 0,
                      fontSize: fs,
                      fontWeight: fw,
                      textAlign: "right",
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis"
                    }}>
                      {prize}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* LEADERBOARD table (center, a bit wider due to larger maxWidth) */}
        <div style={{ flex: 1, minWidth: 0 }}>
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
                background: "#fff",
                borderRadius: 8,
                overflow: "hidden"
              }}
            >
              <thead style={gradientTheadStyle}>
                <tr>
                  {["RANK", "NAME", "NET %", "CAPITAL ($)", "COUNTRY"].map((label, idx) => (
                    <th
                      key={idx}
                      style={{ fontWeight: 1000, fontSize: "16px", padding: "10px 6px", whiteSpace: "nowrap", color: "#fff" }}
                    >
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
                    const globalRank = startIndex + rowIndex;

                    const zebra = { background: rowIndex % 2 === 0 ? "#ffffff" : "#f9f9f9" };
                    const highlight = rowStyleForRank(globalRank);
                    const rowStyle = { ...zebra, ...highlight };

                    let rowFontSize = "14px";
                    let rowFontWeight = 400;
                    if (globalRank === 0) { rowFontSize = "17px"; rowFontWeight = 800; }
                    else if (globalRank === 1) { rowFontSize = "16px"; rowFontWeight = 700; }
                    else if (globalRank === 2) { rowFontSize = "15px"; rowFontWeight = 600; }

                    const leftAccent = accentForRank(globalRank);

                    const n = numVal(row["PctChange"]);
                    const pctColor = n == null ? "#222" : (n > 0 ? "#1e8e3e" : (n < 0 ? "#d93025" : "#222"));
                    let pctFont = rowFontSize;
                    if (globalRank === 0) pctFont = "calc(17px + 6px)";
                    else if (globalRank === 1) pctFont = "calc(16px + 4px)";
                    else if (globalRank === 2) pctFont = "calc(15px + 2px)";

                    const cellBase = { whiteSpace: "nowrap", fontSize: rowFontSize, fontWeight: rowFontWeight };

                    return (
                      <tr key={rowIndex} style={rowStyle}>
                        <td style={{ ...cellBase, fontWeight: 800, borderLeft: `8px solid ${leftAccent}` }}>
                          <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                            {rankBadge(globalRank) || (globalRank + 1)}
                          </span>
                        </td>
                        <td style={cellBase}>{shortName(row["CUSTOMER NAME"])}</td>
                        <td style={cellBase}>
                          <span style={{ color: pctColor, fontWeight: 800, fontSize: pctFont }}>
                            {fmtPct(n)}
                          </span>
                        </td>
                        <td style={cellBase}>{fmtNumber(row["Plan"], 0)}</td>
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

        {/* COUNTDOWN (right) */}
        <div style={{ flex: "0 0 260px" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 13,
              background: "#fff",
              boxShadow: "0 1px 6px rgba(0,0,0,2)",
              borderRadius: 8,
              overflow: "hidden"
            }}
          >
            <thead style={gradientTheadStyle}>
              <tr>
                <th colSpan={4} style={{ padding: "10px 8px", fontWeight: 900, textAlign: "center", fontSize: 14 }}>
                  LEADERBOARD WEEKLY RESET
                </th>
              </tr>
            </thead>
            <tbody>
              <tr style={{ background: "#fafafa" }}>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>DD</td>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>HH</td>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>MM</td>
                <td style={{ padding: "8px 6px", fontWeight: 700, textAlign: "center" }}>SS</td>
              </tr>
              <tr>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.d)}</td>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.h)}</td>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.m)}</td>
                <td style={{ padding: "10px 6px", textAlign: "center", fontWeight: 900, fontSize: 18 }}>{pad2(tleft.s)}</td>
              </tr>
              <tr>
                <td colSpan={4} style={{ padding: "8px 6px", textAlign: "center", color: "#666", fontSize: 12 }}>
                  NEXT RESET: MONDAY 12:00PM BST
                </td>
              </tr>
            </tbody>
          </table>
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
