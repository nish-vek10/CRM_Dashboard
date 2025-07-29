import subprocess
import os
import shutil
import webbrowser
from pathlib import Path
import time

# === Paths ===
BASE_DIR = Path("C:/Users/anish/PycharmProjects/CRM_Dashboard")
REACT_DIR = BASE_DIR / "crm_dashboard"
JSON_SOURCE = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/merged_data_full_enriched.json")
JSON_DEST = REACT_DIR / "public" / "merged_data_full_enriched.json"

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

// Column definitions
const COLUMNS = [
  { label: "Customer Name", key: "Name" },
  { label: "Customer ID", key: "lv_maintpaccountidName" },
  { label: "Account ID", key: "Lv_name" },
  { label: "Email", key: "EMailAddress1" },
  { label: "Phone Code", key: "Lv_Phone1CountryCode" },
  { label: "Phone Number", key: "Lv_Phone1Phone" },
  {
    label: "Country",
    key: "lv_countryidName",
    render: (val) => val ? getFlagImage(val) : ""
  },
  { label: "Affiliate", key: "Lv_SubAffiliate" },
  { label: "Tag", key: "Lv_Tag1" },
  { label: "Plan", key: "Plan" },
  { label: "Plan SB", key: "Plan_SB" },
  { label: "Balance", key: "Balance" },
  { label: "Equity", key: "Equity" },
  { label: "OpenPnL", key: "OpenPnL" }
];

// Sortable fields
const SORT_OPTIONS = [
  { label: "Customer Name", key: "Name" },
  { label: "Customer ID", key: "lv_maintpaccountidName" },
  { label: "Country", key: "lv_countryidName" }
];

function getFlagImage(countryName) {
  const countryMap = {
    "Albania": "al", "Australia": "au", "Bahrain": "bh", "Bangladesh": "bd", "Benin": "bj",
    "Botswana": "bw", "Burkina Faso": "bf", "Burundi": "bi", "Cameroon": "cm", "Canada": "ca",
    "Colombia": "co", "Cote D'Ivoire": "ci", "Cyprus": "cy", "Egypt": "eg", "Ethiopia": "et",
    "France": "fr", "Germany": "de", "Ghana": "gh", "India": "in", "Ireland": "ie",
    "Israel": "il", "Jordan": "jo", "Kenya": "ke", "Lesotho": "ls", "Malaysia": "my",
    "Malta": "mt", "Nepal": "np", "Netherlands": "nl", "Nigeria": "ng", "Pakistan": "pk",
    "Saudi Arabia": "sa", "Senegal": "sn", "Singapore": "sg", "Somalia": "so",
    "South Africa": "za", "Spain": "es", "Swaziland": "sz", "Tanzania": "tz", "Uganda": "ug",
    "United Arab Emirates": "ae", "United Kingdom": "gb", "Uzbekistan": "uz", "Zambia": "zm",
    "Zimbabwe": "zw"
  };

  const code = countryMap[countryName];
  if (!code) return countryName;

  return (
    <span style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: "6px" }}>
      <img
        src={`https://flagcdn.com/w40/${code}.png`}
        alt={countryName}
        style={{ width: "20px", height: "14px", objectFit: "cover", borderRadius: "2px" }}
      />
      {countryName}
    </span>
  );
}

function App() {
  const [originalData, setOriginalData] = useState([]);
  const [data, setData] = useState([]);
  const [sortKey, setSortKey] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 100;

  useEffect(() => {
    fetch("/merged_data_full_enriched.json")
      .then((res) => res.json())
      .then((json) => {
        setOriginalData(json);
        setData(json);
      })
      .catch(console.error);
  }, []);

  const handleSort = (key) => {
    setSortKey(key);
    const sorted = [...data].sort((a, b) => {
      const valA = a[key] || "";
      const valB = b[key] || "";
      return valA.toString().localeCompare(valB.toString());
    });
    setData(sorted);
    setCurrentPage(1);
  };

  const resetSort = () => {
    setData(originalData);
    setSortKey("");
    setSearchQuery("");
    setCurrentPage(1);
  };

  const handleSearch = (e) => {
    const query = e.target.value.toLowerCase();
    setSearchQuery(query);
    const filtered = originalData.filter((row) =>
      Object.values(row).some((val) =>
        String(val).toLowerCase().includes(query)
      )
    );
    setData(filtered);
    setCurrentPage(1);
  };

  const totalPages = Math.ceil(data.length / rowsPerPage);
  const paginatedData = data.slice(
    (currentPage - 1) * rowsPerPage,
    currentPage * rowsPerPage
  );

  const goToPage = (page) => {
    if (page >= 1 && page <= totalPages) {
      setCurrentPage(page);
    }
  };

  return (
    <div style={{ padding: "20px", fontFamily: "'Segoe UI', sans-serif", background: "#fafafa" }}>
    <h1
  className="heading-slide-fade"
  style={{
    fontSize: "3.5rem",
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
      <p
        style={{
          fontSize: "1rem",
          marginBottom: "20px",
          textAlign: "center",
          color: "#666"
        }}
      >
        Total Records: {data.length}
      </p>

      <div style={{ marginBottom: "20px", display: "flex", flexWrap: "wrap", gap: "12px", alignItems: "center",
       justifyContent: "center" }}>
        <input
          type="text"
          placeholder="Search..."
          value={searchQuery}
          onChange={handleSearch}
          style={{ 
            padding: "10px 14px", 
            width: "220px", 
            border: "1px solid #ccc", 
            borderRadius: "6px",
            fontSize: "14px",
            fontFamily: "'Segoe UI', sans-serif",
            boxShadow: "1px 1px 5px rgba(0,0,0,0.05)",
            outline: "none"
          }}
        />
        <select
    onChange={(e) => handleSort(e.target.value)}
    value={sortKey}
    style={{
      padding: "10px 14px",
      border: "1px solid #ccc",
      borderRadius: "6px",
      fontSize: "14px",
      fontFamily: "'Segoe UI', sans-serif",
      boxShadow: "1px 1px 5px rgba(0,0,0,0.05)",
      outline: "none"
    }}
  >
    <option value="">Sort By</option>
    {SORT_OPTIONS.map((opt) => (
      <option key={opt.key} value={opt.key}>
        {opt.label}
      </option>
    ))}
  </select>

  <button
    onClick={resetSort}
    style={{
      padding: "10px 18px",
      border: "none",
      backgroundColor: "#3498db",
      color: "#fff",
      borderRadius: "6px",
      fontWeight: "600",
      fontSize: "14px",
      fontFamily: "'Segoe UI', sans-serif",
      cursor: "pointer",
      boxShadow: "1px 1px 5px rgba(0,0,0,0.05)",
      transition: "background-color 0.3s"
    }}
    onMouseEnter={(e) => e.target.style.backgroundColor = "#2980b9"}
    onMouseLeave={(e) => e.target.style.backgroundColor = "#3498db"}
  >
    Reset
  </button>
</div>
      <div style={{ overflowX: "auto", maxHeight: "75vh", overflowY: "scroll" }}>
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
              {COLUMNS.map((col, index) => (
                <th key={index}>{col.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paginatedData.map((row, rowIndex) => (
              <tr key={rowIndex} style={{ background: rowIndex % 2 === 0 ? "#ffffff" : "#f9f9f9" }}>
                {COLUMNS.map((col, colIndex) => (
                  <td key={colIndex}>
                    {col.render ? col.render(row[col.key]) : row[col.key]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: "20px" }}>
        <button
          onClick={() => goToPage(currentPage - 1)}
          disabled={currentPage === 1}
          style={{ marginRight: "10px", padding: "6px 10px", cursor: "pointer" }}
        >
          Prev
        </button>
        <span style={{ margin: "0 10px" }}>
          Page {currentPage} of {totalPages}
        </span>
        <button
          onClick={() => goToPage(currentPage + 1)}
          disabled={currentPage === totalPages}
          style={{ marginLeft: "10px", padding: "6px 10px", cursor: "pointer" }}
        >
          Next
        </button>
      </div>
    </div>
  );
}

export default App;
""", encoding="utf-8")

# === Start React App ===
print("[OK] Starting React server...")
subprocess.Popen(["C:/Program Files/nodejs/npm.cmd", "start"], cwd=REACT_DIR, shell=True)

# === Open Browser ===
time.sleep(5)
webbrowser.open("http://localhost:3000")
