import subprocess
import os
import shutil
import webbrowser
from pathlib import Path

# === 1. Paths ===
BASE_DIR = Path("C:/Users/anish/PycharmProjects/CRM_Dashboard")
REACT_DIR = BASE_DIR / "crm_dashboard"
JSON_SOURCE = Path("C:/Users/anish/Desktop/Anish/CRM API/CRM Dashboard/merged_clean_data.json")
JSON_DEST = REACT_DIR / "public" / "merged_clean_data.json"
shutil.copy(JSON_SOURCE, JSON_DEST)

# === 2. Check if React app exists ===
if not REACT_DIR.exists():
    print("[OK] Creating new React app...")
    subprocess.run(["npx", "create-react-app", "crm_dashboard"], cwd=BASE_DIR, shell=True)

# === 3. Ensure JSON file is copied ===
print("[OK] Copying JSON data...")
os.makedirs(REACT_DIR / "public", exist_ok=True)
shutil.copy(JSON_SOURCE, JSON_DEST)

# === 4. Write App.js with Pagination ===
APP_JS = REACT_DIR / "src" / "App.js"
APP_JS.write_text("""\
import React, { useEffect, useState } from "react";
import "./App.css";

function App() {
  const [data, setData] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 200;

  useEffect(() => {
    fetch("/merged_clean_data.json")
      .then((res) => res.json())
      .then(setData)
      .catch(console.error);
  }, []);

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
    <div className="App" style={{ padding: "20px", fontFamily: "sans-serif" }}>
      <h1>CRM Dashboard</h1>
      <p>Total Records: {data.length}</p>
      <h2>Page {currentPage} of {totalPages}</h2>

      <div style={{ overflowX: "auto", maxHeight: "75vh", overflowY: "scroll" }}>
        <table border="1" cellPadding="5">
          <thead>
            <tr>
              {paginatedData.length > 0 &&
                Object.keys(paginatedData[0]).map((col, index) => (
                  <th key={index}>{col}</th>
                ))}
            </tr>
          </thead>
          <tbody>
            {paginatedData.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {Object.values(row).map((val, colIndex) => (
                  <td key={colIndex}>{val}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: "20px" }}>
        <button onClick={() => goToPage(currentPage - 1)} disabled={currentPage === 1}>
          Prev
        </button>
        <span style={{ margin: "0 10px" }}>
          Page {currentPage} of {totalPages}
        </span>
        <button onClick={() => goToPage(currentPage + 1)} disabled={currentPage === totalPages}>
          Next
        </button>
      </div>
    </div>
  );
}

export default App;
""")


# === 5. Install dependencies and start React app ===
print("[OK] Starting React server...")
subprocess.Popen(["C:/Program Files/nodejs/npm.cmd", "start"], cwd=REACT_DIR, shell=True)

# === 6. Open browser after delay ===
import time
time.sleep(5)
webbrowser.open("http://localhost:3000")
