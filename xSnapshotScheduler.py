import asyncio
import datetime
from pathlib import Path
from playwright.async_api import async_playwright

# Where to save screenshots
OUT_DIR = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard/finalCleanOutput/snapshots")
OUT_DIR.mkdir(parents=True, exist_ok=True)

URL = "http://localhost:3001"

SCHEDULE_HOURS = [0, 4, 8, 12, 16, 20]  # local time
SCHEDULE_MINUTE = 35

INITIAL_STARTUP_DELAY_SEC = 5 * 60  # 5 minutes


def format_hms(seconds: int) -> str:
    """Format seconds as HH:MM:SS."""
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def seconds_until_next_run() -> int:
    now = datetime.datetime.now()
    today = now.date()
    for h in SCHEDULE_HOURS:
        t = datetime.datetime.combine(today, datetime.time(hour=h, minute=SCHEDULE_MINUTE))
        if t > now:
            return int((t - now).total_seconds())
    # none left today -> next day 00:30
    t = datetime.datetime.combine(today + datetime.timedelta(days=1),
                                  datetime.time(hour=SCHEDULE_HOURS[0], minute=SCHEDULE_MINUTE))
    return int((t - now).total_seconds())


async def capture_once():
    ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_file = OUT_DIR / f"dashboard_{ts}.png"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            viewport={"width": 1600, "height": 900},
            device_scale_factor=1
        )
        page = await ctx.new_page()
        # Allow the app to load (and data fetch to complete)
        await page.goto(URL, wait_until="networkidle", timeout=120000)
        # small extra wait to ensure final paints settle (optional)
        await page.wait_for_timeout(2000)
        await page.screenshot(path=str(out_file), full_page=True)
        await browser.close()
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] [OK] Saved {out_file}")


async def main_loop():
    # Initial delay to give React time to boot
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Waiting {format_hms(INITIAL_STARTUP_DELAY_SEC)} for React to start...")
    await asyncio.sleep(INITIAL_STARTUP_DELAY_SEC)

    while True:
        secs = seconds_until_next_run()
        now = datetime.datetime.now()
        next_run = now + datetime.timedelta(seconds=secs)
        print(
            f"[{now.strftime('%H:%M:%S')}] Next capture at {next_run.strftime('%H:%M:%S')} "
            f"(in {format_hms(secs)})"
        )
        await asyncio.sleep(max(1, secs))  # sleep until the slot
        try:
            await capture_once()
        except Exception as e:
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] [ERR] Snapshot failed: {e}")
        # loop continues to schedule the next one


if __name__ == "__main__":
    asyncio.run(main_loop())
