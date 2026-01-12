# taichung-mortuary-sync
台中殯葬業者資料同步工具
import streamlit as st
import pandas as pd
import sqlite3
import requests
import io
from datetime import datetime

# 資料來源（台中市開放資料）
DATA_URL = "[https://opendata.taichung.gov.tw/dataset/5d10fdbb-7812-431e-93f0-c7bb84dfeccc/resource/463a6e9a-7c97-4089-9e8c-57c25c345b54/download]"

# 資料庫檔名
DB_NAME = "mortuary.db"
TABLE_NAME = "legal_businesses"

# 欄位對應表（防範官方欄位名稱改變）
FIELD_MAP = {
    "統一編號": "統一編號",
    "公司名稱": ["公司商號名稱", "公司名稱", "名稱"],
    "負責人": ["負責人"],
    "公司電話": ["聯絡電話", "電話", "公司電話"],
    "公司地址": ["營業地址", "地址", "公司地址"],
    "電子信箱": ["電子信箱", "Email"]
}

def get_column(df, possible_names, default='N/A'):
    for col in possible_names:
        if col in df.columns:
            return df[col].fillna(default)
    return pd.Series([default] * len(df))

@st.cache_resource
def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            "統一編號" TEXT PRIMARY KEY,
            "公司名稱" TEXT,
            "負責人" TEXT,
            "公司電話" TEXT,
            "公司地址" TEXT,
            "電子信箱" TEXT,
            "最後更新時間" TEXT
        )
    ''')
    return conn

def sync_data():
    st.info("正在從台中市政府開放資料下載最新資料...")
    
    try:
        resp = requests.get(DATA_URL, timeout=30)
        resp.raise_for_status()

        # 處理中文編碼（最常出問題的地方）
        try:
            content = resp.content.decode('utf-8-sig')
        except:
            content = resp.content.decode('big5', errors='replace')

        df = pd.read_csv(io.StringIO(content))
        df.columns = df.columns.str.strip()

        # 清洗資料
        data = {k: get_column(df, v) if isinstance(v, list) else df.get(v, pd.Series(['N/A']*len(df))) 
                for k, v in FIELD_MAP.items()}
        clean_df = pd.DataFrame(data)
        clean_df = clean_df[clean_df['統一編號'].notna() & (clean_df['統一編號'].str.strip() != '')]

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = init_db()
        cursor = conn.cursor()

        inserted = 0
        for _, row in clean_df.iterrows():
            cursor.execute(f'''
                INSERT OR REPLACE INTO {TABLE_NAME}
                ("統一編號","公司名稱","負責人","公司電話","公司地址","電子信箱","最後更新時間")
                VALUES (?,?,?,?,?,?,?)
            ''', (
                row['統一編號'],
                row['公司名稱'],
                row['負責人'],
                row['公司電話'],
                row['公司地址'],
                row['電子信箱'],
                current_time
            ))
            inserted += 1

        conn.commit()
        conn.close()

        return f"同步成功！共處理 {inserted} 筆資料"
    except Exception as e:
        return f"同步失敗：{str(e)}"

# ──────────────── 網頁介面 ────────────────
st.set_page_config(page_title="台中殯葬業者資料工具", layout="wide")
st.title("台中合法殯葬禮儀業者資料同步工具")
st.caption("資料來源：臺中市政府開放資料平台")

# 初始化資料庫（只跑一次）
if 'db_init' not in st.session_state:
    init_db()
    st.session_state.db_init = True

# 同步按鈕
if st.button("🔄 立即同步最新資料", type="primary", use_container_width=True):
    with st.spinner("正在同步資料，請稍等 10-30 秒..."):
        result = sync_data()
        if "成功" in result:
            st.success(result)
        else:
            st.error(result)

# 顯示最新資料（前 50 筆）
if st.checkbox("查看最新同步的資料（前 50 筆）"):
    try:
        conn = sqlite3.connect(DB_NAME)
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY 最後更新時間 DESC LIMIT 50", conn)
        st.dataframe(df, use_container_width=True)
        conn.close()
    except:
        st.info("還沒有資料，請先點上方同步按鈕")

# 下載完整資料
try:
    conn = sqlite3.connect(DB_NAME)
    final_df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
    csv = final_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    
    st.download_button(
        label="📥 下載完整最新業者名冊 (CSV 檔)",
        data=csv,
        file_name=f"台中殯葬業者名冊_{datetime.now():%Y%m%d}.csv",
        mime="text/csv",
        use_container_width=True
    )
    conn.close()
except:
    st.info("還沒有資料可下載，請先同步")
