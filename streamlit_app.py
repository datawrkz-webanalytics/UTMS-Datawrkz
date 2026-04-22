"""
UTMS — Single-File Application
===================================================
Consolidated Streamlit app housing UTMS Generator and DOCAD Engine.
Dual-bot authentication: UTMS via google-credentials.json, DOCAD via docad-credentials.json.

Run:  streamlit run app.py
Deps: pip install streamlit gspread google-auth pandas
"""

import datetime
import os
import urllib.parse
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd

import gspread
from google.oauth2.service_account import Credentials

# ═══════════════════════════════════════════════════════════
# PAGE CONFIG (must be the first Streamlit command)
# ═══════════════════════════════════════════════════════════

st.set_page_config(
    page_title="UTMS",
    page_icon="🏠",
    layout="wide",
)

# ═══════════════════════════════════════════════════════════
# DUAL-BOT GOOGLE AUTHENTICATION (Cached)
# ═══════════════════════════════════════════════════════════

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_utms_client():
    """UTMS bot — Cloud secrets first, local JSON fallback."""
    try:
        creds_dict = dict(st.secrets["utms_gcp"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except (KeyError, FileNotFoundError):
        pass

    # Local fallback
    if os.path.exists("google-credentials.json"):
        creds = Credentials.from_service_account_file("google-credentials.json", scopes=SCOPES)
        return gspread.authorize(creds)

    st.error("❌ UTMS credentials not found. Add [utms_gcp] in Streamlit Secrets or provide google-credentials.json locally.")
    st.stop()

@st.cache_resource
def get_docad_client():
    """DOCAD bot — Cloud secrets first, local JSON fallback."""
    try:
        creds_dict = dict(st.secrets["docad_gcp"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    except (KeyError, FileNotFoundError):
        pass

    # Local fallback
    if os.path.exists("docad-credentials.json"):
        creds = Credentials.from_service_account_file("docad-credentials.json", scopes=SCOPES)
        return gspread.authorize(creds)

    # Not critical — app can still work for UTMS only
    return None

# Initialize
utms_gc = get_utms_client()
docad_gc = get_docad_client()

# ═══════════════════════════════════════════════════════════
# UTMS — SCHEMA & PLATFORM TEMPLATES
# ═══════════════════════════════════════════════════════════

SCHEMA = {
    "templateName": "Granular Ad-Level Tracking",
    "fields": [
        {
            "id": "landing_page",
            "label": "Landing Page URL",
            "type": "text",
            "placeholder": "https://...",
            "required": True,
        },
        {
            "id": "client",
            "label": "Client Name",
            "type": "creatable-select",
            "required": True,
        },
        {
            "id": "platform",
            "label": "Platform",
            "type": "dropdown",
            "options": ["meta", "linkedin", "dv360", "adsp", "google", "tiktok", "reddit", "quora", "x", "snapchat", "tradedesk"],
            "required": True,
        },
        {
            "id": "objective",
            "label": "Campaign Objective",
            "type": "dropdown",
            "options": ["lead", "traffic", "conversion","reach"],
            "required": True,
        },
        {
            "id": "theme",
            "label": "Campaign Theme",
            "type": "text",
            "placeholder": "e.g., Q3Promo",
            "required": True,
        },
        {
            "id": "aud",
            "label": "Audience",
            "type": "text",
            "placeholder": "e.g., Retargeting30D",
            "required": True,
        },
        {
            "id": "creative",
            "label": "Creatives (Comma-separated)",
            "type": "text",
            "placeholder": "e.g., Static_V1, Reel_V2",
            "required": False,
        },
    ],
}

# Platforms that hide the Audience field and relabel Theme → Campaign Name
CAMPAIGN_NAME_ONLY_PLATFORMS = {"dv360", "google"}

PLATFORM_TEMPLATES = {
    # ── Original platforms (utm_medium includes paid{objective}) ──
    "meta":      "utm_source=dw_meta&utm_medium={type}paid{objective}&utm_campaign={theme}{aud}{creative}&utm_content={{campaign.id}}{{adset.id}}{{ad.id}}_{{placement}}",
    "linkedin":  "utm_source=dw_linkedin&utm_medium={type}paid{objective}&utm_campaign={theme}{aud}{creative}&utm_content={{CAMPAIGN_GROUP_ID}}{{CAMPAIGN_ID}}{{CREATIVE_ID}}",
    # ── Google & DV360 (same structure, only utm_content macros differ) ──
    "dv360":     "utm_source=dw_google&utm_medium=cpc&utm_campaign={theme}{aud}&utm_content=${CAMPAIGN_ID}${INSERTION_ORDER_ID}${LINE_ITEM_ID}${CREATIVE_ID}",
    "google":    "utm_source=dw_google&utm_medium=cpc&utm_campaign={theme}{aud}&utm_content={campaignid}{adgroupid}_{creative}",
    # ── Programmatic ──
    "adsp":      "utm_source=dw_adsp&utm_medium={type}paid{objective}&utm_campaign={theme}{aud}&utm_content={%campaign_cfid}{%ad_cfid}_{%creative_cfid}",
    "tradedesk": "utm_source=dw_tradedesk&utm_medium={type}paid{objective}&utm_campaign={theme}{aud}&utm_content=%%TTD_CAMPAIGNID%%%%TTD_ADGROUPID%%%%TTD_CREATIVEID%%%%TTD_PUBLISHER_NAME%%_%%TTD_SITE%%",
    # ── Social platforms ──
    "tiktok":    "utm_source=dw_tiktok&utm_medium={type}&utm_campaign={theme}{aud}{creative}&utm_content=_CAMPAIGN_ID_AID_CID_",
    "reddit":    "utm_source=dw_reddit&utm_medium={type}&utm_campaign={theme}{aud}{creative}&utm_content={{campaign.id}}{{adgroup.id}}{{ad.id}}",
    "quora":     "utm_source=dw_quora&utm_medium={type}&utm_campaign={theme}{aud}&utm_content={{campaign.id}}{{adset.id}}_{{ad.id}}",
    "x":         "utm_source=dw_x&utm_medium={type}&utm_campaign={theme}{aud}{creative}&utm_content={{tw_campaignid}}{{tw_adgroupid}}{{tw_adid}}",
    "snapchat":  "utm_source=dw_snapchat&utm_medium={type}&utm_campaign={theme}{aud}{creative}&utm_content={{campaign.id}}{{adset.id}}{{ad.id}}",
}

GOOGLE_CAMPAIGN_TYPES = ["Search", "Display", "Performance Max", "Video"]

# UTMS Sheets target
UTMS_SPREADSHEET_ID = "1xnQ6LHWEjY5FMapJCx3EEAykDfJf80deq6Fk2R7WQpU"

# ═══════════════════════════════════════════════════════════
# UTMS — HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════


def get_utms_spreadsheet():
    """Return the UTMS spreadsheet object."""
    return utms_gc.open_by_key(UTMS_SPREADSHEET_ID)


def get_existing_clients():
    """Return list of client (tab) names from the UTMS spreadsheet."""
    try:
        ss = get_utms_spreadsheet()
        return [ws.title for ws in ss.worksheets()]
    except Exception:
        return []


HEADER_ROW = ["Date", "Time", "Platform", "Campaign", "Creative", "URL"]


def _stamp_headers(ws):
    """Force-insert the header row into Row 1 and apply bold formatting."""
    ws.insert_row(HEADER_ROW, index=1)
    try:
        ws.format("A1:F1", {"textFormat": {"bold": True}})
    except Exception as e:
        print(f"Header formatting skipped: {e}")


def log_to_sheets(client_name: str, platform: str, campaign: str, creative: str, url: str):
    """Log a single UTM entry to the client's tab in Google Sheets."""
    try:
        ss = get_utms_spreadsheet()

        try:
            ws = ss.worksheet(client_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = ss.add_worksheet(title=client_name, rows=1000, cols=7)
            _stamp_headers(ws)

        if len(ws.get_all_values()) == 0:
            _stamp_headers(ws)

        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
        formatted_date = now.strftime("%d/%m/%Y")
        formatted_time = now.strftime("%I:%M:%S %p")

        ws.append_row(
            [formatted_date, formatted_time, platform, campaign, creative, url],
            value_input_option="RAW",
        )
        return True
    except Exception as e:
        st.error(f"Sheets logging failed: {e}")
        return False


def _calculate_media_type(platform: str) -> str:
    """Auto-calculate Media Type from platform."""
    if platform in ("meta", "linkedin", "tiktok", "reddit", "quora", "x", "snapchat"):
        return "social"
    elif platform in ("tradedesk", "adsp"):
        return "prog"
    elif platform in ("dv360", "google"):
        return "cpc"
    return ""


def generate_utm_urls(schema: dict, values: dict) -> list[dict]:
    """Generate UTM-tagged URLs from schema + form values."""
    platform = values.get("platform", "")
    landing_page = (values.get("landing_page") or "").strip()

    if not platform or not landing_page:
        return []

    raw_creatives = values.get("creative", "")
    is_google = platform == "google"
    is_dv360 = platform == "dv360"

    if is_google or is_dv360:
        creatives = ["Google_Ad"] if is_google else ["DV360_Ad"]
    else:
        creatives = [c.strip() for c in raw_creatives.split(",") if c.strip()]

    if not creatives:
        return []

    is_google = platform == "google"
    google_campaign_type = values.get("google_campaign_type", "") if is_google else ""
    calculated_type = _calculate_media_type(platform)

    url_template = PLATFORM_TEMPLATES.get(platform, "")
    if not url_template:
        return []

    if is_google and google_campaign_type == "Search":
        url_template += "&utm_term={keyword}"

    results = []
    for creative_name in creatives:
        utm_string = url_template

        # Inject auto-calculated type
        utm_string = utm_string.replace("{type}", urllib.parse.quote(calculated_type, safe=""))

        for field in schema["fields"]:
            fid = field["id"]
            if fid in ("landing_page", "client"):
                continue
            placeholder = "{" + fid + "}"
            value = creative_name if fid == "creative" else (values.get(fid) or "")
            utm_string = utm_string.replace(placeholder, urllib.parse.quote(value, safe=""))

        if is_google and google_campaign_type == "Search":
            utm_string = utm_string.replace("{keyword}", "{keyword}")

        separator = "&" if "?" in landing_page else "?"
        full_url = f"{landing_page}{separator}{utm_string}"

        results.append({"creative": creative_name, "url": full_url})

    return results


def copy_to_clipboard_js(text: str):
    """Inject JS that breaks out of the iframe to copy via parent document."""
    escaped = text.replace("\\", "\\\\").replace("", "\\").replace("$", "\\$")
    components.html(
        f"""
        <script>
        const textArea = window.parent.document.createElement("textarea");
        textArea.value = {escaped};
        textArea.style.position = "absolute";
        textArea.style.left = "-999999px";
        window.parent.document.body.appendChild(textArea);
        textArea.select();
        try {{
            window.parent.document.execCommand('copy');
        }} catch (error) {{
            console.error('Fallback clipboard failed', error);
        }} finally {{
            window.parent.document.body.removeChild(textArea);
        }}
        </script>
        """,
        height=0, width=0,
    )


# ═══════════════════════════════════════════════════════════
# DOCAD — HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════


def fetch_templates():
    """Fetch the Templates tab from DOCAD_Admin_Control and return as DataFrame."""
    try:
        spreadsheet = docad_gc.open("DOCAD_Admin_Control")
        templates_sheet = spreadsheet.worksheet("Templates")
        data = templates_sheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to load Templates: {e}")
        return pd.DataFrame()


def fetch_admin_control():
    """Fetch the DOCAD_Admin_Control main sheet and return as DataFrame."""
    try:
        spreadsheet = docad_gc.open("DOCAD_Admin_Control")
        admin_sheet = spreadsheet.sheet1
        data = admin_sheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to load Admin Control: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════
# CUSTOM CSS
# ═══════════════════════════════════════════════════════════

CUSTOM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=Fira+Code:wght@400;500&display=swap');

    .stApp {
        font-family: 'Inter', sans-serif !important;
    }
    #MainMenu, footer, header {visibility: hidden;}

    .glass-card {
        background: var(--secondary-background-color);
        border: 1px solid var(--secondary-background-color);
        border-radius: 1.5rem;
        padding: 2.5rem;
        margin-bottom: 1.5rem;
    }
    .section-title {
        font-size: 11px; font-weight: 900; text-transform: uppercase;
        letter-spacing: 0.2em; color: var(--text-color) !important; margin-bottom: 1.5rem;
    }
    .url-card {
        border-radius: 1rem; background: var(--secondary-background-color);
        border: 1px solid var(--secondary-background-color); overflow: hidden;
        margin-bottom: 1rem;
    }
    .url-card-header {
        display: flex; align-items: center; justify-content: space-between;
        padding: 0.875rem 1.5rem; border-bottom: 1px solid var(--secondary-background-color);
    }
    .creative-label {
        font-size: 10px; font-weight: 900; text-transform: uppercase;
        letter-spacing: 0.2em; color: var(--text-color) !important; opacity: 0.6;
    }
    .creative-name {
        margin-left: 0.75rem; font-size: 0.875rem; font-weight: 700; color: var(--text-color) !important;
    }
    .url-body {
        padding: 1rem 1.5rem; font-family: 'Fira Code', monospace;
        font-size: 0.8125rem; color: #4f46e5; word-break: break-all; line-height: 1.8;
    }
    .stSelectbox > div > div, .stTextInput > div > div > input {
        background-color: transparent !important;
        border: 1px solid var(--text-color) !important;
        border-radius: 0.75rem !important; font-size: 0.9375rem !important;
        color: var(--text-color) !important;
    }
    .stSelectbox > div > div:focus-within, .stTextInput > div > div:focus-within {
        border-color: rgba(251,146,60,0.5) !important;
        box-shadow: 0 0 0 4px rgba(251,146,60,0.1) !important;
    }
    .stSelectbox label, .stTextInput label {
        font-size: 0.6875rem !important; font-weight: 800 !important;
        text-transform: uppercase !important; letter-spacing: 0.15em !important;
        color: var(--text-color) !important; opacity: 0.7;
    }
    .stButton > button {
        border: none !important; border-radius: 0.75rem !important;
        font-weight: 600 !important; font-size: 0.875rem !important;
        padding: 0.75rem 1.5rem !important; transition: all 0.2s !important;
    }
    .stButton > button[kind="primary"] {
        background: #0f172a !important; color: #ffffff !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: #1e293b !important; transform: translateY(-1px);
    }
    .stButton > button[kind="secondary"] {
        background: var(--secondary-background-color) !important; color: var(--text-color) !important;
        border: 1px solid var(--secondary-background-color) !important;
    }
    .count-badge {
        display: inline-flex; align-items: center;
        background: var(--secondary-background-color); padding: 0.25rem 0.75rem;
        border-radius: 9999px; border: 1px solid var(--secondary-background-color);
        font-size: 11px; font-weight: 600; color: var(--text-color) !important; margin-left: 0.75rem;
    }
    .new-client-badge {
        display: inline-block; margin-top: 0.375rem; font-size: 0.6875rem;
        font-weight: 600; color: #d97706; letter-spacing: 0.05em;
    }
    .footer-text {
        text-align: center; color: var(--text-color) !important; opacity: 0.3; font-size: 10px;
        font-weight: 500; letter-spacing: 0.2em; text-transform: uppercase;
        margin-top: 3rem; padding-bottom: 2rem;
    }
    .empty-state {
        text-align: center; padding: 4rem 0; color: var(--text-color) !important; opacity: 0.5; font-size: 0.875rem;
    }
</style>
"""

# ═══════════════════════════════════════════════════════════
# APP LAYOUT
# ═══════════════════════════════════════════════════════════

st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ── Header ──
st.markdown(
    """
    <div style="display:flex;align-items:center;gap:0.75rem;margin-bottom:0.5rem;">
        <span style="font-size:1.75rem;font-weight:900;color:var(--text-color);letter-spacing:-0.025em;">
            UTMS
        </span>
        <span style="font-size:0.6875rem;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:0.1em;">
            powered by datawrkz
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ═══════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════

if "logged_urls" not in st.session_state:
    st.session_state.logged_urls = set()

# ═══════════════════════════════════════════════════════════
# UTMS GENERATOR (primary interface)
# ═══════════════════════════════════════════════════════════

schema = SCHEMA
existing_clients = get_existing_clients()
selected_platform = ""  # will be set by the platform dropdown

# ── Configure Parameters ──
st.subheader("Configure Parameters")

values = {}

for field in schema["fields"]:
    fid = field["id"]
    label = field["label"]
    required = field.get("required", False)

    # ── Conditional field visibility/labelling for dv360 & google ──
    current_platform = values.get("platform", "")
    is_campaign_name_only = current_platform in CAMPAIGN_NAME_ONLY_PLATFORMS

    if fid == "aud" and is_campaign_name_only:
        values[fid] = ""  # hidden — set empty so templates get a clean string
        continue

    # ── Hide creatives field for Google (and dv360) ──
    if fid == "creative" and current_platform in CAMPAIGN_NAME_ONLY_PLATFORMS:
        continue

    if fid == "theme" and is_campaign_name_only:
        label = "Campaign Name"

    display_label = f"{label} :red[*]" if required else label

    if field["type"] == "dropdown":
        options = field.get("options", [])

        values[fid] = st.selectbox(
            display_label,
            options=[""] + options,
            format_func=lambda x, lbl=label: f"Select {lbl}…" if x == "" else x,
            key=f"utms_field_{fid}",
        )

        # ── Google conditional: Campaign Type selector ──
        if fid == "platform" and values.get("platform") == "google":
            values["google_campaign_type"] = st.selectbox(
                "Google Campaign Type 🔴",
                options=[""] + GOOGLE_CAMPAIGN_TYPES,
                format_func=lambda x: "Select Campaign Type…" if x == "" else x,
                key="utms_field_google_campaign_type",
            )

    elif field["type"] == "creatable-select":
        values[fid] = st.text_input(
            display_label,
            placeholder=field.get("placeholder", "Type or select a client…"),
            key=f"utms_field_{fid}",
        )
        if existing_clients:
            with st.expander("📋 Existing clients", expanded=False):
                st.caption(", ".join(existing_clients))
        client_val = values.get(fid, "").strip()
        if client_val and client_val not in existing_clients:
            st.markdown(
                '<span class="new-client-badge">⚡ New client — a new sheet tab will be created</span>',
                unsafe_allow_html=True,
            )

    elif field["type"] == "text":
        values[fid] = st.text_input(
            display_label,
            placeholder=field.get("placeholder", ""),
            key=f"utms_field_{fid}",
        )

# ── Safety: clear google_campaign_type when platform is not google ──
if values.get("platform") != "google":
    values.pop("google_campaign_type", None)

# ── Check all required fields filled (skip hidden "aud" for campaign-name-only platforms) ──
current_platform = values.get("platform", "")
skip_for_required = set()
if current_platform in CAMPAIGN_NAME_ONLY_PLATFORMS:
    skip_for_required.add("aud")

all_required = all(
    (values.get(f["id"]) or "").strip()
    for f in schema["fields"]
    if f.get("required") and f["id"] not in skip_for_required
)

if not all_required:
    st.info("Fill all required fields and at least one creative to generate URLs.")

# ── Generate URLs ──
results = generate_utm_urls(schema, values)

# ── Generated URLs ──
st.subheader("Generated URLs")

if not results:
    st.caption("🔗 Fill in the fields above to generate tracking URLs.")
else:
    # ── Copy All & Log (with duplicate guard) ──
    if st.button("📋 Copy All & Log", type="primary", key="utms_copy_all"):
        client_name = (values.get("client") or "").strip()
        campaign = f"{values.get('theme', '')}_{values.get('aud', '')}"
        platform = values.get("platform", "")
        all_urls = "\n".join(r["url"] for r in results)

        new_urls = [r for r in results if r["url"] not in st.session_state.logged_urls]
        dup_urls = [r for r in results if r["url"] in st.session_state.logged_urls]

        for r in new_urls:
            log_to_sheets(client_name, platform, campaign, r["creative"], r["url"])
            st.session_state.logged_urls.add(r["url"])

        copy_to_clipboard_js(all_urls)

        if new_urls:
            st.success(f"Logged {len(new_urls)} URL(s) to Google Sheets.")
        if dup_urls:
            st.warning(f"⚠️ {len(dup_urls)} URL(s) already logged — skipped to avoid duplicates.")

    st.markdown("---")

    # ── Individual URL cards ──
    for idx, result in enumerate(results):
        st.markdown(
            f"""
            <div class="url-card">
                <div class="url-card-header">
                    <div>
                        <span class="creative-label">Ad Creative:</span>
                        <span class="creative-name">{result["creative"]}</span>
                    </div>
                </div>
                <div class="url-body">{result["url"]}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if st.button(f"📋 Copy & Log", key=f"utms_copy_{idx}"):
            client_name = (values.get("client") or "").strip()
            campaign = f"{values.get('theme', '')}_{values.get('aud', '')}"
            platform = values.get("platform", "")

            if result["url"] in st.session_state.logged_urls:
                st.warning("⚠️ This URL has already been logged. Please avoid duplicate entries.")
            else:
                log_to_sheets(client_name, platform, campaign, result["creative"], result["url"])
                st.session_state.logged_urls.add(result["url"])
            copy_to_clipboard_js(result["url"])

# ── Sheets Link ──
st.markdown("---")
st.markdown(
    """
    <div style="text-align:center;margin:1.5rem 0;">
        <a href="https://docs.google.com/spreadsheets/d/1xnQ6LHWEjY5FMapJCx3EEAykDfJf80deq6Fk2R7WQpU/edit?gid=1343757584#gid=1343757584"
           target="_blank"
           style="display:inline-flex;align-items:center;gap:0.5rem;
                  background:rgba(255,255,255,0.45);backdrop-filter:blur(20px);
                  padding:0.75rem 1.5rem;border-radius:0.75rem;
                  border:1px solid rgba(255,255,255,0.6);
                  text-decoration:none;color:#0f172a;font-weight:600;font-size:0.875rem;
                  box-shadow:0 4px 12px rgba(0,0,0,0.06);transition:all 0.2s;">
            📊 Open UTMS Logger Sheet
        </a>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Footer ──
st.markdown(
    '<div class="footer-text">UTMS • Powered by Datawrkz • 2026</div>',
    unsafe_allow_html=True,
)