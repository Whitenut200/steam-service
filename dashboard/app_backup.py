"""Steam 게임 대시보드 — Streamlit 프로토타입

실행:
  cd D:\project\steam_service
  streamlit run dashboard/app.py
"""
import sys
import os

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from utils.env import init_env
init_env()

import html
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime, timezone

# ── 설정 ──────────────────────────────────────────────────
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
DATASET = os.getenv("BQ_DATASET", "steam_data")
TABLE = f"{PROJECT_ID}.{DATASET}"


@st.cache_resource
def get_client():
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=3600)
def load_game_list():
    query = f"""
        SELECT app_id, name, header_image, genres, developers, publishers,
               release_date, is_free, metacritic_score,categories,short_description
        FROM `{TABLE}.games`
        ORDER BY name
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_review_summary(app_id: int):
    query = f"""
        SELECT *
        FROM `{TABLE}.review_summary`
        WHERE app_id = {app_id}
        LIMIT 1
    """
    df = get_client().query(query).to_dataframe()
    return df.iloc[0] if len(df) > 0 else None


@st.cache_data(ttl=600)
def load_player_counts(app_id: int, days: int = 30):
    query = f"""
        SELECT collected_at, player_count
        FROM `{TABLE}.player_counts`
        WHERE app_id = {app_id}
          AND TIMESTAMP(collected_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        ORDER BY collected_at
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_price_history(app_id: int):
    query = f"""
        SELECT snapshot_date, initial_price, final_price, discount_percent, currency
        FROM `{TABLE}.price_history`
        WHERE app_id = {app_id}
        ORDER BY snapshot_date
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_player_heatmap(app_id: int, days: int = 12):
    """동접 히트맵용: KST 기준 날짜 × 3시간 bin 집계 + 해당일 할인율"""
    query = f"""
        WITH p AS (
          SELECT
            DATE(TIMESTAMP(collected_at), "Asia/Seoul") AS day,
            FLOOR(EXTRACT(HOUR FROM TIMESTAMP(collected_at) AT TIME ZONE "Asia/Seoul") / 3) * 3 AS hour_bin,
            player_count
          FROM `{TABLE}.player_counts`
          WHERE app_id = {app_id}
            AND TIMESTAMP(collected_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        )
        SELECT day, hour_bin, AVG(player_count) AS player_count
        FROM p
        GROUP BY day, hour_bin
        ORDER BY day, hour_bin
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=600)
def load_player_stats_14d(app_id: int):
    """최근 14일 동접 최고/최저"""
    query = f"""
        SELECT MAX(player_count) AS peak, MIN(player_count) AS low
        FROM `{TABLE}.player_counts`
        WHERE app_id = {app_id}
          AND TIMESTAMP(collected_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    """
    df = get_client().query(query).to_dataframe()
    if df.empty or pd.isna(df.iloc[0]["peak"]):
        return 0, 0
    return int(df.iloc[0]["peak"]), int(df.iloc[0]["low"])


@st.cache_data(ttl=3600)
def load_news(app_id: int, limit: int = 20):
    query = f"""
        SELECT title, url, date, feed_label, author
        FROM `{TABLE}.news`
        WHERE app_id = {app_id}
        ORDER BY date DESC
        LIMIT {limit}
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_avg_playtime(app_id: int):
    """리뷰 작성자 기준 평균 플레이타임 (분 → 시간)"""
    query = f"""
        SELECT AVG(playtime_forever) as avg_min
        FROM `{TABLE}.reviews`
        WHERE app_id = {app_id} AND playtime_forever > 0
    """
    df = get_client().query(query).to_dataframe()
    if df.empty or pd.isna(df.iloc[0]["avg_min"]):
        return 0
    return int(df.iloc[0]["avg_min"]) // 60  # 시간 단위


@st.cache_data(ttl=3600)
def load_reviews_by_day(app_id: int, days: int = 7):
    """최근 N일 일별 리뷰 수 + 추천/비추천 (KST 기준)"""
    query = f"""
        SELECT
            DATE(TIMESTAMP_SECONDS(timestamp_created), "Asia/Seoul") as day,
            COUNT(*) as cnt,
            COUNTIF(voted_up = TRUE) as positive,
            COUNTIF(voted_up = FALSE) as negative
        FROM `{TABLE}.reviews`
        WHERE app_id = {app_id}
          AND TIMESTAMP_SECONDS(timestamp_created) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        GROUP BY day
        ORDER BY day
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_reviews_list(app_id: int, limit: int = 300):
    """리뷰 목록 (필터/정렬은 in-memory 처리)"""
    query = f"""
        SELECT review_text, review_text_ko, voted_up, language,
               timestamp_created, playtime_forever
        FROM `{TABLE}.reviews`
        WHERE app_id = {app_id}
          AND review_text IS NOT NULL
          AND TRIM(review_text) != ''
        ORDER BY timestamp_created DESC
        LIMIT {limit}
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_playtime_at_review_7d(app_id: int, days: int = 7):
    """최근 N일 일별 평균 playtime_at_review (추천/비추천 분리, 시간 단위)"""
    query = f"""
        SELECT
            DATE(TIMESTAMP_SECONDS(timestamp_created), "Asia/Seoul") as day,
            voted_up,
            AVG(playtime_at_review) / 60.0 as avg_hours,
            COUNT(*) as cnt
        FROM `{TABLE}.reviews`
        WHERE app_id = {app_id}
          AND TIMESTAMP_SECONDS(timestamp_created) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
          AND playtime_at_review > 0
        GROUP BY day, voted_up
        ORDER BY day
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_language_by_vote(app_id: int):
    """언어별 리뷰수 (추천/비추천 분리)"""
    query = f"""
        SELECT voted_up, language, COUNT(*) as cnt
        FROM `{TABLE}.reviews`
        WHERE app_id = {app_id} AND language IS NOT NULL
        GROUP BY voted_up, language
        ORDER BY cnt DESC
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_keyword_analysis(app_id: int, polarity: str, limit: int = 20):
    """키워드 분석 결과 (최신 날짜 기준, polarity별 top N)"""
    query = f"""
        SELECT keyword, pos_tag, count, ratio
        FROM `{TABLE}.keyword_analysis`
        WHERE app_id = {app_id}
          AND polarity = '{polarity}'
          AND collected_date = (
              SELECT MAX(collected_date)
              FROM `{TABLE}.keyword_analysis`
              WHERE app_id = {app_id}
          )
        ORDER BY count DESC
        LIMIT {limit}
    """
    return get_client().query(query).to_dataframe()


@st.cache_data(ttl=3600)
def load_category_analysis(app_id: int):
    """카테고리별 감성분석 결과 (최신 날짜 기준)"""
    query = f"""
        SELECT polarity, category, subcategory, SUM(count) as count
        FROM `{TABLE}.category_analysis`
        WHERE app_id = {app_id}
          AND collected_date = (
              SELECT MAX(collected_date)
              FROM `{TABLE}.category_analysis`
              WHERE app_id = {app_id}
          )
        GROUP BY polarity, category, subcategory
        ORDER BY count DESC
    """
    return get_client().query(query).to_dataframe()


# ── 공통 유틸 ────────────────────────────────────────────
def ts_to_kst_date(ts: int) -> str:
    """UNIX timestamp → YYYY-MM-DD 문자열 (UTC 기준; KST 변환 필요시 tz 파라미터)"""
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")


def render_title_with_legend(title: str, legend_items: list = None):
    """카드 레이블 크기 제목(#aaa 15px) + 오른쪽 정렬 범례(흰색 13px + 색 동그라미)"""
    legend_html = ""
    if legend_items:
        legend_html = "".join(
            f'<span style="color:#fff; font-size:13px; margin-left:14px; display:inline-flex; align-items:center;">'
            f'<span style="display:inline-block; width:10px; height:10px; background:{c}; '
            f'border-radius:50%; margin-right:5px;"></span>{l}</span>'
            for l, c in legend_items
        )
    st.markdown(
        f'<div style="display:flex; justify-content:space-between; align-items:center; '
        f'margin-top:40px; margin-bottom:8px;">'
        f'<div style="color:#fff; font-size:1.5rem; font-weight:bold;">{title}</div>'
        f'<div>{legend_html}</div></div>',
        unsafe_allow_html=True,
    )


def render_scrollable_table(headers: list, rows_html: str, height: int = 400, extra_style: str = ""):
    """스크롤 고정 높이 테이블. headers = [(제목, width_px_or_None), ...]"""
    thead = "".join(
        f'<th style="padding:8px 12px; text-align:left; color:#aaa;'
        f'{f" width:{w}px;" if w else ""}">{h}</th>'
        for h, w in headers
    )
    st.markdown(
        f'<div style="height:{height}px; overflow-y:auto; border-radius:8px; '
        f'border:1px solid #313D4C;{extra_style}">'
        f'<table style="width:100%; border-collapse:collapse;">'
        f'<thead><tr style="background:#365268; position:sticky; top:0;">{thead}</tr></thead>'
        f'<tbody>{rows_html}</tbody></table></div>',
        unsafe_allow_html=True,
    )


# ── 페이지 설정 ──────────────────────────────────────────
st.set_page_config(page_title="Steam Dashboard", layout="wide")
st.markdown("""
  <style>
      /* 전체 배경 */
      .stApp { background-color: #1B2838 !important; }
      /* 기본 글자색 */
      .stApp, .stApp * { color: #fff; }
      /* 표 스타일 */
      table { width: 100%; background: #313D4C; border-collapse: collapse; }
      td, th { border: 1px solid #1B2838; }
      th { padding: 8px 12px; color: #fff !important; background: #365268 !important; }
      td { padding: 4px 8px; color: #16B2E2 !important; }
      td:first-child { width: 80px; white-space: nowrap; }
      /* 게이지 툴팁 */
      .gauge-wrap { position:relative; }
      .gauge-tooltip {
          display:none; position:absolute; top:calc(100% + 8px); left:50%;
          transform:translateX(-50%); background:#313D4C; color:#fff; padding:8px 14px;
          border-radius:8px; font-size:13px; white-space:nowrap; z-index:10;
          box-shadow:0 2px 8px rgba(0,0,0,0.4);
      }
      .gauge-wrap:hover .gauge-tooltip { display:block; }
      /* Streamlit 위젯 배경 투명 */
      .stSelectbox > div > div { background: #313D4C; }
      .stExpander { border-color: #313D4C; }
      /* 전체 세로 블록 간격 축소 (Streamlit 기본 1rem → 0.5rem) */
      section[data-testid="stMain"] [data-testid="stVerticalBlock"] { gap: 0.5rem; }
      /* 팝오버 패널(stPopoverBody) 배경 */
      div[data-testid="stPopoverBody"],
      div[data-baseweb="popover"] > div {
          background-color: #323E4D !important;
          border: none !important;
      }
      /* 버튼/팝오버/expander 다크 테마 고정 (focus/active에서 흰색 되는 현상 차단) */
      .stButton > button,
      .stDownloadButton > button,
      button[data-testid="stBaseButton-secondary"],
      [data-testid="stPopover"] button,
      div[data-testid="stExpander"] details summary {
          background-color: #313D4C !important;
          color: #fff !important;
          border: 1px solid #313D4C !important;
      }
      .stButton > button:hover,
      .stDownloadButton > button:hover,
      button[data-testid="stBaseButton-secondary"]:hover,
      [data-testid="stPopover"] button:hover,
      div[data-testid="stExpander"] details summary:hover {
          background-color: #365268 !important;
          color: #fff !important;
          border-color: #365268 !important;
      }
      .stButton > button:focus,
      .stButton > button:focus-visible,
      .stButton > button:active,
      .stDownloadButton > button:focus,
      button[data-testid="stBaseButton-secondary"]:focus,
      button[data-testid="stBaseButton-secondary"]:focus-visible,
      button[data-testid="stBaseButton-secondary"]:active,
      [data-testid="stPopover"] button:focus,
      [data-testid="stPopover"] button:focus-visible,
      [data-testid="stPopover"] button:active,
      div[data-testid="stExpander"] details summary:focus,
      div[data-testid="stExpander"] details summary:focus-visible {
          background-color: #313D4C !important;
          color: #fff !important;
          outline: none !important;
          box-shadow: none !important;
      }
      /* Streamlit 도구설명(help tooltip) 테두리 제거 */
      div[data-baseweb="tooltip"] > div,
      div[data-testid="stTooltipContent"] {
          border: none !important;
          box-shadow: 0 2px 8px rgba(0,0,0,0.4) !important;
      }
  </style>
  """, unsafe_allow_html=True)
st.title("Steam Game Dashboard")

# ── 게임 검색 + 탭 ───────────────────────────────────────
games_df = load_game_list()

if games_df.empty:
    st.error("게임 데이터가 없습니다.")
    st.stop()

game_options = {f"{row['name']} ({row['app_id']})": row['app_id'] for _, row in games_df.iterrows()}

search_col, tab_col = st.columns([2, 3])
with search_col:
    selected_label = st.selectbox("게임 검색", options=list(game_options.keys()), index=0)
with tab_col:
    st.markdown('<div style="margin-top:28px;"></div>', unsafe_allow_html=True)
    tab = st.radio(
        "탭",
        ["Main", "Detail", "Detail2"],
        horizontal=True,
        label_visibility="collapsed",
    )

selected_app_id = game_options[selected_label]
game_info = games_df[games_df["app_id"] == selected_app_id].iloc[0]

st.divider()

# ── 평가 색상 매핑 (양쪽 탭 공통) ─────────────────────────
SCORE_COLORS = {
    "Overwhelmingly Positive": ("#9cc36a", "압도적으로 긍정적"),
    "Very Positive": ("#78a843", "매우 긍정적"),
    "Positive": ("#5f8f2f", "긍정적"),
    "Mostly Positive": ("#4c6b22", "대체로 긍정적"),
    "Mixed": ("#4f8ea3", "복합적"),
    "Mostly Negative": ("#5a1a1a", "대체로 부정적"),
    "Negative": ("#8b2e2e", "부정적"),
    "Very Negative": ("#a94442", "매우 부정적"),
    "Overwhelmingly Negative": ("#c0563f", "압도적으로 부정적"),
}

# ── 레이아웃: 좌 | 중앙 | 우 (탭별 크기 조정) ─────────────
if tab == "Main":
    col_left, col_mid, col_right = st.columns([1.5, 2, 2], gap="large")
elif tab == "Detail":
    col_left, col_mid, col_right = st.columns([1.5, 2, 2], gap="large")
else:  # Detail2 — 2-col로 우측 영역 안에서 카드+히트맵 stacking (히트맵이 카드 바로 아래로 오도록)
    col_left, col_content = st.columns([1.5, 4.1], gap="large")

# ── 좌측: 게임 정보 (양쪽 탭 공통) ──────────────────────
with col_left:
    title_col, icon_col = st.columns([5, 1])
    with title_col:
        st.subheader(game_info["name"])
    with icon_col:
        with st.popover("🤖", use_container_width=False, help="AI 코멘트"):
            st.markdown(
                '<div style="color:#fff; font-size:1.5rem; font-weight:bold; margin-bottom:16px;">AI 코멘트</div>',
                unsafe_allow_html=True,
            )
            popover_card = lambda title, body: (
                f'<div style="background:#1B2838; border-radius:12px; '
                f'padding:18px 20px; margin-bottom:14px; min-width:380px;">'
                f'<div style="color:#fff; font-size:15px; font-weight:bold; margin-bottom:10px;">{title}</div>'
                f'<div style="color:#ccc; font-size:13px; line-height:1.6;">{body}</div></div>'
            )
            st.markdown(popover_card(
                "감성 분석 요약",
                "긍정 ##% — 자주 언급된 단어: 협동, 재미 ...<br>"
                "부정 ##% — 자주 언급된 단어: 버그, 렉 ...<br>"
                "<i style='color:#666;'>(번역 완료 후 Kiwi 기반 분석으로 활성화)</i>"
            ), unsafe_allow_html=True)
            st.markdown(popover_card(
                "추천 영향 요인 (로지스틱 회귀)",
                "추천(0/1)에 영향이 큰 요인 Top-5<br>"
                "<i style='color:#666;'>(분석 모델 구현 예정)</i>"
            ), unsafe_allow_html=True)
            st.markdown(popover_card(
                "이상 탐지",
                "평소 대비 부정 반응 추이 알림<br>"
                '<span style="color:#c0563f;">예: 최근 7일간 부정 비율이 평소보다 ##% 증가</span><br>'
                "<i style='color:#666;'>(시계열 이상치 탐지 구현 예정)</i>"
            ), unsafe_allow_html=True)

    header_img = game_info.get("header_image", "")
    if header_img and isinstance(header_img, str) and header_img.startswith("http"):
        st.image(header_img, use_container_width=True)

    st.markdown(f"""
    | 항목 | 내용 |
    |------|------|
    | **개발사** | {game_info.get('developers', '-')} |
    | **배급사** | {game_info.get('publishers', '-')} |
    | **장르** | {game_info.get('genres', '-')} |
    | **출시일** | {game_info.get('release_date', '-')} |
    | **메타크리틱** | {game_info.get('metacritic_score', '-')} |
    | **무료여부** | {'무료' if game_info.get('is_free') else '유료'} |
    | **게임 설명** | {game_info.get('short_description', '-')} |
    """)

    # 카테고리 배지 (전체 표시)
    categories = game_info.get('categories', '')
    if categories and isinstance(categories, str):
        cat_list = [c.strip() for c in categories.split(",") if c.strip()]
        badge_style = "display:inline-block; background:#313D4C; color:#fff; padding:4px 10px; margin:2px; margin-top:10px; border-radius:12px; font-size:15px;"
        badges_html = "".join(f'<span style="{badge_style}">{c}</span>' for c in cat_list)
        st.markdown('<div style="margin-top:10px; margin-left:5px;font-weight:bold;">카테고리</div>', unsafe_allow_html=True)
        st.markdown(badges_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════
# 탭별 컨텐츠
# ══════════════════════════════════════════════════════════

if tab == "Main":
    # ── 중앙: 리뷰 + 카테고리 + 패치노트 ──────────────────
    with col_mid:
        st.markdown('<div style="margin-top:60px;"></div>', unsafe_allow_html=True)
        summary = load_review_summary(selected_app_id)
        if summary is not None:
            total = int(summary.get("total_reviews", 0))
            positive = int(summary.get("total_positive", 0))
            negative = int(summary.get("total_negative", 0))
            score_desc = summary.get("review_score_desc", "")
            if total > 0:
                pos_ratio = positive / total
                pct = pos_ratio * 100
                color, ko_desc = SCORE_COLORS.get(score_desc, ("#888", score_desc))

                st.markdown(f"""<div style="display:flex; gap:8px; height:110px;">
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px;">
<div style="color:#aaa; font-size:13px;">총 리뷰</div>
<div style="display:flex; align-items:center; gap:16px; margin-top:8px;">
<div style="color:#fff; font-size:20px; font-weight:bold; white-space:nowrap;">{total:,}개</div>
<div class="gauge-wrap" style="flex:1; background:#333; border-radius:6px; height:36px; overflow:visible; position:relative; cursor:pointer;">
<div style="background:linear-gradient(90deg, #4c6b22, #5a7d2a); width:{pct:.0f}%; height:100%; border-radius:6px;"></div>
<span style="position:absolute; top:50%; left:50%; transform:translate(-50%,-50%); color:#fff; font-size:14px; font-weight:bold; text-shadow:0 0 4px rgba(0,0,0,0.8);">{pct:.0f}%</span>
<div class="gauge-tooltip">👍 긍정 {positive:,}개 ({pct:.1f}%) &nbsp;|&nbsp; 👎 부정 {negative:,}개 ({100-pct:.1f}%)</div>
</div>
</div>
</div>
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px; display:flex; flex-direction:column; justify-content:center;">
<div style="color:#aaa; font-size:13px;">통합 평가</div>
<div style="display:flex; align-items:baseline; gap:8px; margin-top:8px;">
<span style="color:{color}; font-size:20px; font-weight:bold;">{ko_desc}</span>
</div>
</div>
</div>""", unsafe_allow_html=True)
            else:
                st.info("리뷰 데이터 없음")
        else:
            st.info("리뷰 요약 데이터 없음")

        # 카테고리별 레이더 차트 (긍정/부정 좌우 분리, 하위 카테고리 7축)
        st.subheader(" ")
        st.subheader("카테고리별 분석")
        cat_df = load_category_analysis(selected_app_id)
        if not cat_df.empty:
            POS_MERGE = {
                "재미/몰입": ["재미", "중독/몰입", "리플레이"],
                "스토리/세계관": ["스토리", "세계관/캐릭터"],
                "자유도": ["자유도"],
                "그래픽": ["그래픽"],
                "사운드/조작감": ["사운드", "조작감"],
                "콘텐츠": ["콘텐츠"],
                "가성비/멀티": ["가성비", "멀티플레이"],
            }
            NEG_MERGE = {
                "버그/크래시": ["버그", "크래시"],
                "핵/치트": ["핵/치트"],
                "최적화/서버": ["최적화", "서버"],
                "밸런스": ["밸런스"],
                "난이도": ["난이도"],
                "반복성": ["반복성"],
                "콘텐츠/과금": ["콘텐츠부족", "과금", "UI/UX"],
            }

            def merge_subcategories(df, polarity, merge_map):
                sub_df = df[df["polarity"] == polarity]
                sub_map = {row["subcategory"]: row["count"] for _, row in sub_df.iterrows()}
                return {label: sum(sub_map.get(s, 0) for s in subs) for label, subs in merge_map.items()}

            pos_values = merge_subcategories(cat_df, "positive", POS_MERGE)
            neg_values = merge_subcategories(cat_df, "negative", NEG_MERGE)

            def make_radar(values_dict, color, fill_color):
                labels = list(values_dict.keys())
                values = list(values_dict.values())
                fig = go.Figure()
                fig.add_trace(go.Scatterpolar(
                    r=values + [values[0]], theta=labels + [labels[0]],
                    fill="toself", fillcolor=fill_color,
                    line=dict(color=color, width=2),
                ))
                fig.update_layout(
                    polar=dict(
                        bgcolor="#1B2838",
                        radialaxis=dict(visible=True, gridcolor="#2a3a4a", tickfont=dict(color="#aaa", size=9)),
                        angularaxis=dict(gridcolor="#2a3a4a", tickfont=dict(color="#fff", size=11)),
                    ),
                    paper_bgcolor="#1B2838",
                    margin=dict(t=10, b=10, l=0, r=10),
                    height=375,
                    showlegend=False,
                )
                return fig

            radar_left, radar_right = st.columns(2)
            with radar_left:
                st.markdown('<div style="color:#fff; font-size:14px; font-weight:bold; margin-top:10px; padding-bottom:4px;">긍정</div>', unsafe_allow_html=True)
                st.plotly_chart(make_radar(pos_values, "#4CAF50", "rgba(76,175,80,0.15)"), use_container_width=True)
            with radar_right:
                st.markdown('<div style="color:#fff; font-size:14px; font-weight:bold; margin-top:10px; padding-bottom:4px;">부정</div>', unsafe_allow_html=True)
                st.plotly_chart(make_radar(neg_values, "#F44336", "rgba(244,67,54,0.15)"), use_container_width=True)
        else:
            fig_placeholder = go.Figure()
            fig_placeholder.add_annotation(text="카테고리 분석 데이터 없음", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False, font=dict(size=14, color="#888"))
            fig_placeholder.update_layout(
                paper_bgcolor="#1B2838", plot_bgcolor="#1B2838",
                margin=dict(t=20, b=10, l=0, r=10), height=290,
                xaxis=dict(visible=False), yaxis=dict(visible=False),
            )
            st.plotly_chart(fig_placeholder, use_container_width=True)

        # 패치노트 / 뉴스
        st.subheader(" ")
        st.subheader("패치노트 / 뉴스")
        news_df = load_news(selected_app_id)
        if not news_df.empty:
            rows_html = ""
            for _, n in news_df.iterrows():
                date_str = ts_to_kst_date(n["date"])
                url = n.get("url", "")
                title = n.get("title", "(제목 없음)")
                title_cell = (
                    f'<a href="{url}" target="_blank" style="color:#16B2E2; text-decoration:none;">{title}</a>'
                    if url else title
                )
                rows_html += f"<tr><td style='white-space:nowrap;'>{date_str}</td><td>{title_cell}</td></tr>"

            render_scrollable_table(
                headers=[("날짜", 100), ("제목", None)],
                rows_html=rows_html,
            )
        else:
            st.info("뉴스 데이터 없음")

    # ── 우측: 가격 + 동시접속자 + 할인 내역 ──────────────
    with col_right:
        st.markdown('<div style="margin-top:60px;"></div>', unsafe_allow_html=True)

        price_df = load_price_history(selected_app_id)
        player_df = load_player_counts(selected_app_id)

        # 가격 정보 (가장 최근 날짜)
        if not price_df.empty:
            price_df["snapshot_date"] = pd.to_datetime(price_df["snapshot_date"])
            latest_price = price_df.sort_values("snapshot_date").iloc[-1]
            final_price = int(latest_price["final_price"])
            initial_price = int(latest_price["initial_price"])
            discount = int(latest_price["discount_percent"])
        else:
            final_price, initial_price, discount = 0, 0, 0

        # 동시접속자 평균
        if not player_df.empty:
            player_df["collected_at"] = pd.to_datetime(player_df["collected_at"])
            avg_players = int(player_df["player_count"].mean())
            peak_players = int(player_df["player_count"].max())
        else:
            avg_players, peak_players = 0, 0

        # 가격 표시
        if final_price > 0:
            price_html = f'<span style="color:#fff; font-size:20px; font-weight:bold;">{final_price:,}원</span>'
            if discount > 0:
                price_html += f' <span style="color:#888; font-size:14px; text-decoration:line-through; margin-left:4px;">{initial_price:,}원</span>'
                price_html += f' <span style="background:#4c6b22; color:#fff; padding:2px 8px; border-radius:4px; font-size:13px; font-weight:bold; margin-left:8px;">-{discount}%</span>'
            else:
                price_html += f' <span style="background:#555; color:#fff; padding:2px 8px; border-radius:4px; font-size:13px; font-weight:bold; margin-left:8px;">0%</span>'
        else:
            price_html = '<span style="color:#aaa; font-size:20px; font-weight:bold;">무료 / 정보없음</span>'

        # 동시접속자 표시
        if avg_players > 0:
            player_html = f"""
                <span style="color:#fff; font-size:20px; font-weight:bold;">{avg_players:,}명</span>
                <span style="color:#888; font-size:12px; margin-left:8px;">최고 {peak_players:,}명</span>
            """
        else:
            player_html = '<span style="color:#aaa; font-size:28px; font-weight:bold;">데이터 없음</span>'

        st.markdown(f"""<div style="display:flex; gap:8px; height:110px;">
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px;">
<div style="color:#aaa; font-size:13px;">현재 가격</div>
<div style="margin-top:8px;">{price_html}</div>
</div>
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px;">
<div style="color:#aaa; font-size:13px;">평균 동시접속자 <span style="color:#666; font-size:11px;">(30일)</span></div>
<div style="margin-top:8px;">{player_html}</div>
</div>
</div>""", unsafe_allow_html=True)

        # 동시접속자 라인차트
        st.subheader(" ")
        st.subheader("동시접속자 추이")
        if not player_df.empty:
            fig_player = px.line(player_df, x="collected_at", y="player_count")
            fig_player.update_traces(line_color="#4c6b22")
            fig_player.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc",
                margin=dict(t=20, b=10, l=0, r=10),
                height=400,
                xaxis=dict(gridcolor="#333", title=""),
                yaxis=dict(gridcolor="#333", title=""),
            )
            st.plotly_chart(fig_player, use_container_width=True)
        else:
            st.info("동시접속자 데이터 없음")

        # 가격 히스토리 (할인 내역) — price_df 재사용
        st.subheader(" ")
        st.subheader("할인 내역")
        if not price_df.empty:
            price_hist = price_df.sort_values("snapshot_date").drop_duplicates("snapshot_date")
            date_range = pd.date_range(price_hist["snapshot_date"].min(), price_hist["snapshot_date"].max(), freq="D")
            price_hist = price_hist.set_index("snapshot_date").reindex(date_range).ffill().reset_index()
            price_hist.rename(columns={"index": "snapshot_date"}, inplace=True)
            price_hist = price_hist[price_hist["discount_percent"] > 0]

            rows_html = ""
            for _, row in price_hist.sort_values("snapshot_date", ascending=False).iterrows():
                date_str = str(row["snapshot_date"])[:10]
                init_p = f'{int(row["initial_price"]):,}원'
                final_p = f'{int(row["final_price"]):,}원'
                disc = int(row["discount_percent"])
                disc_html = (
                    f'<span style="color:#16B2E2; font-weight:bold;">-{disc}%</span>'
                    if disc > 0 else '<span style="color:#888;">0%</span>'
                )
                rows_html += (
                    f"<tr><td style='white-space:nowrap;'>{date_str}</td>"
                    f"<td>{init_p}</td><td>{final_p}</td><td>{disc_html}</td></tr>"
                )

            render_scrollable_table(
                headers=[("날짜", None), ("정가", None), ("할인가", None), ("할인율", None)],
                rows_html=rows_html,
            )
        else:
            st.info("가격 변동 데이터 없음")

elif tab == "Detail":
    # ══════════════════════════════════════════════════════
    # Tab: Detail (리뷰 & 플레이타임 분석)
    # ══════════════════════════════════════════════════════

    POS_COLORS = ["#4c6b22", "#5f8f2f", "#78a843", "#9cc36a", "#b8d98a", "#888"]
    NEG_COLORS = ["#5a1a1a", "#8b2e2e", "#a94442", "#c0563f", "#d67a65", "#888"]

    summary = load_review_summary(selected_app_id)
    if summary is not None:
        total = int(summary.get("total_reviews", 0))
        positive = int(summary.get("total_positive", 0))
        score_desc = summary.get("review_score_desc", "")
    else:
        total, positive, score_desc = 0, 0, ""
    pos_pct = (positive / total * 100) if total > 0 else 0
    color, ko_desc = SCORE_COLORS.get(score_desc, ("#888", score_desc or "데이터 없음"))
    avg_hours = load_avg_playtime(selected_app_id)

    # ── 중앙: 카드 2개 + 추천/비추천 7일 평균 플레이타임 + 리뷰수 bar ──
    with col_mid:
        st.markdown('<div style="margin-top:60px;"></div>', unsafe_allow_html=True)

        # 카드 2개: 총 리뷰수 / 평가 (긍정 % 레이블 옆)
        st.markdown(f"""<div style="display:flex; gap:8px; height:110px;">
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px; display:flex; flex-direction:column; justify-content:center;">
<div style="color:#aaa; font-size:13px;">총 리뷰</div>
<div style="color:#fff; font-size:24px; font-weight:bold; margin-top:8px;">{total:,}개</div>
</div>
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px; display:flex; flex-direction:column; justify-content:center;">
<div style="color:#aaa; font-size:13px;">통합 평가</div>
<div style="display:flex; align-items:baseline; gap:10px; margin-top:8px;">
<span style="color:{color}; font-size:18px; font-weight:bold;">{ko_desc}</span>
<span style="color:#fff; font-size:13px;">긍정 {pos_pct:.0f}%</span>
</div>
</div>
</div>""", unsafe_allow_html=True)

        # 중간1 좌: 7일 평균 playtime_at_review (추천/비추천 라인)
        render_title_with_legend(
            "최근 7일 평균 플레이 타임",
            [("추천", "#78a843"), ("비추천", "#c0563f")],
        )
        pt_df = load_playtime_at_review_7d(selected_app_id, days=7)
        if not pt_df.empty:
            pt_df["day"] = pd.to_datetime(pt_df["day"])
            fig_pt = go.Figure()
            for voted_up, label, line_color in [(True, "추천", "#78a843"), (False, "비추천", "#c0563f")]:
                sub = pt_df[pt_df["voted_up"] == voted_up]
                if not sub.empty:
                    fig_pt.add_trace(go.Scatter(
                        x=sub["day"], y=sub["avg_hours"],
                        mode="lines+markers", name=label,
                        line=dict(color=line_color, width=2),
                        marker=dict(color=line_color, size=8),
                        hovertemplate=f"<b>%{{x|%Y-%m-%d}}</b><br>{label} 평균: %{{y:.1f}}h<extra></extra>",
                    ))
            fig_pt.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc", margin=dict(t=20, b=10, l=0, r=10),
                height=300, showlegend=False,
                xaxis=dict(gridcolor="#333", title=""),
                yaxis=dict(gridcolor="#333", title="시간"),
                hoverlabel=dict(bgcolor="#323E4D", font_size=13, font_color="#fff"),
            )
            st.plotly_chart(fig_pt, use_container_width=True)
        else:
            st.info("최근 7일 playtime_at_review 데이터 없음")

        # 중간2 좌: 리뷰수 stacked bar (추천/비추천)
        render_title_with_legend(
            "최근 7일 리뷰수",
            [("추천", "#78a843"), ("비추천", "#c0563f")],
        )
        daily_df = load_reviews_by_day(selected_app_id, days=7)
        if not daily_df.empty:
            daily_df["day"] = pd.to_datetime(daily_df["day"])
            daily_df["positive"] = daily_df["positive"].astype(int)
            daily_df["negative"] = daily_df["negative"].astype(int)
            # 일별 긍정 비율 계산
            daily_df["total"] = daily_df["positive"] + daily_df["negative"]
            daily_df["pos_ratio"] = daily_df.apply(
                lambda r: round(r["positive"] / r["total"] * 100) if r["total"] > 0 else 0,
                axis=1,
            ).astype(int)

            fig_cnt = go.Figure()
            fig_cnt.add_trace(go.Bar(
                x=daily_df["day"], y=daily_df["positive"], name="추천",
                marker_color="#78a843",
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>추천: %{y}개<extra></extra>",
            ))
            fig_cnt.add_trace(go.Bar(
                x=daily_df["day"], y=daily_df["negative"], name="비추천",
                marker_color="#c0563f",
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>비추천: %{y}개<extra></extra>",
            ))
            # 스택 위에 긍정 비율 레이블
            for _, r in daily_df.iterrows():
                if r["total"] > 0:
                    fig_cnt.add_annotation(
                        x=r["day"], y=r["total"],
                        text=f"{r['pos_ratio']}%",
                        showarrow=False, yshift=12,
                        font=dict(color="#ccc", size=11),
                    )
            fig_cnt.update_layout(
                barmode="stack",
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc", margin=dict(t=20, b=10, l=0, r=10),
                height=300, showlegend=False,
                xaxis=dict(gridcolor="#333", title=""),
                yaxis=dict(gridcolor="#333", title=""),
                hoverlabel=dict(bgcolor="#323E4D", font_size=13, font_color="#fff"),
            )
            st.plotly_chart(fig_cnt, use_container_width=True)
        else:
            st.info("최근 7일 리뷰 데이터 없음")

    # ── 우측: 카드 2개 + 언어 파이 2개 + 키워드 빈도(공란) ──
    with col_right:
        st.markdown('<div style="margin-top:60px;"></div>', unsafe_allow_html=True)

        # 카드 2개: 평균 플레이타임 / 긍정 키워드 개수(공란)
        st.markdown(f"""<div style="display:flex; gap:8px; height:110px;">
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px; display:flex; flex-direction:column; justify-content:center;">
<div style="color:#aaa; font-size:13px;">평균 플레이타임</div>
<div style="color:#fff; font-size:24px; font-weight:bold; margin-top:8px;">{avg_hours:,}시간</div>
</div>
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px; display:flex; flex-direction:column; justify-content:center;">
<div style="color:#aaa; font-size:13px;">긍정 키워드 개수</div>
<div style="color:#666; font-size:20px; font-weight:bold; margin-top:8px;">— <span style="font-size:12px;">분석 준비 중</span></div>
</div>
</div>""", unsafe_allow_html=True)

        # 중간1 우: 언어 비중 파이 2개 (추천/비추천, Top 5 + 기타 hover)
        st.markdown(
            '<div style="color:#fff; font-size:1.5rem; font-weight:bold; margin-top:40px; margin-bottom:8px;">'
            '언어 비중 (추천 / 비추천)</div>',
            unsafe_allow_html=True,
        )
        lang_df = load_language_by_vote(selected_app_id)

        def prepare_pie(df_v, top_n=5):
            if df_v.empty:
                return [], [], []
            ds = df_v.sort_values("cnt", ascending=False)
            top = ds.head(top_n)
            others = ds.iloc[top_n:]
            labels = top["language"].tolist()
            values = top["cnt"].astype(int).tolist()
            customdata = [""] * len(labels)
            if len(others) > 0:
                labels.append("기타")
                values.append(int(others["cnt"].sum()))
                detail = "<br>".join(
                    f"{r['language']}: {int(r['cnt']):,}" for _, r in others.iterrows()
                )
                customdata.append("<br>" + detail)
            return labels, values, customdata

        pie_cols = st.columns([1, 1])
        for i, (v, title, accent, palette) in enumerate([
            (True, "추천", "#78a843", POS_COLORS), (False, "비추천", "#c0563f", NEG_COLORS)
        ]):
            with pie_cols[i]:
                st.markdown(f'<div style="color:#fff; font-size:14px; font-weight:bold; margin-top:5px;">{title}</div>', unsafe_allow_html=True)
                df_v = lang_df[lang_df["voted_up"] == v]
                labels, values, customdata = prepare_pie(df_v)
                if labels:
                    fig = go.Figure(data=[go.Pie(
                        labels=labels, values=values, customdata=customdata,
                        hovertemplate="<b>%{label}</b><br>%{value:,}개 (%{percent})%{customdata}<extra></extra>",
                        marker=dict(colors=palette[:len(labels)]),
                        textinfo="label+percent", textfont_size=10,
                    )])
                    fig.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
                        margin=dict(t=20, b=10, l=0, r=10), height=275,
                        showlegend=False,
                        hoverlabel=dict(bgcolor="#323E4D", font_size=12, font_color="#fff"),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"{title} 데이터 없음")

        # 중간2 우: 키워드 빈도 표 (추천/비추천 좌우)
        TAG_LABEL = {"NNG": "명사", "NNP": "고유", "VA": "형용사"}
        st.markdown(
            '<div style="height:40px;"></div>'
            '<div style="color:#fff; font-size:1.5rem; font-weight:bold; margin-bottom:8px;">'
            '키워드 빈도 (추천 / 비추천)</div>',
            unsafe_allow_html=True,
        )
        pos_kw = load_keyword_analysis(selected_app_id, "positive", 20)
        neg_kw = load_keyword_analysis(selected_app_id, "negative", 20)

        def render_keyword_table(df, accent_color):
            if df.empty:
                return '<div style="color:#666; text-align:center; padding:20px;">데이터 없음</div>'
            rows_html = ""
            for i, (_, r) in enumerate(df.iterrows()):
                bg = "#1e2a36" if i % 2 == 0 else "#1B2838"
                tag = TAG_LABEL.get(r.get("pos_tag", ""), r.get("pos_tag", ""))
                rows_html += (
                    f'<tr style="background:{bg};">'
                    f'<td style="padding:6px 10px; color:#fff; font-size:13px;">{i+1}</td>'
                    f'<td style="padding:6px 10px; color:#fff; font-size:13px;">{r["keyword"]}</td>'
                    f'<td style="padding:6px 10px; color:#888; font-size:12px;">{tag}</td>'
                    f'<td style="padding:6px 10px; color:{accent_color}; font-size:13px; text-align:right;">{int(r["count"]):,}</td>'
                    f'</tr>'
                )
            return (
                f'<div style="height:275px; overflow-y:auto; border-radius:8px; border:1px solid #313D4C;">'
                f'<table style="width:100%; border-collapse:collapse;">'
                f'<thead><tr style="background:#171d25; position:sticky; top:0;">'
                f'<th style="padding:8px 10px; color:#aaa; text-align:left; width:30px;">순위</th>'
                f'<th style="padding:8px 10px; color:#aaa; text-align:left;">키워드</th>'
                f'<th style="padding:8px 10px; color:#aaa; text-align:left;">품사</th>'
                f'<th style="padding:8px 10px; color:#aaa; text-align:right;">빈도</th>'
                f'</tr></thead><tbody>{rows_html}</tbody></table></div>'
            )

        kw_left, kw_right = st.columns(2)
        with kw_left:
            st.markdown('<div style="color:#fff; font-size:14px; font-weight:bold; margin-bottom:6px;margin-top:5px;">추천 키워드 TOP 20</div>', unsafe_allow_html=True)
            st.markdown(render_keyword_table(pos_kw, "#fff"), unsafe_allow_html=True)
        with kw_right:
            st.markdown('<div style="color:#fff; font-size:14px; font-weight:bold; margin-bottom:6px;margin-top:5px;">비추천 키워드 TOP 20</div>', unsafe_allow_html=True)
            st.markdown(render_keyword_table(neg_kw, "#fff"), unsafe_allow_html=True)

    # ── 하단: 리뷰 목록 (중앙+우측 너비) ─────────────────
    spacer_col, table_col = st.columns([1.5, 4])
    with table_col:
        st.subheader(" ")
        st.subheader("리뷰 목록")
        reviews_df = load_reviews_list(selected_app_id, limit=300)

        if reviews_df.empty:
            st.info("리뷰 데이터 없음")
        else:
            f1, f2, f3, f4 = st.columns([1, 1, 1, 1])
            with f1:
                vote_filter = st.radio("추천 여부", ["전체", "추천", "비추천"], horizontal=True, key="vote_f")
            with f2:
                lang_options = ["전체"] + sorted(reviews_df["language"].dropna().unique().tolist())
                lang_filter = st.selectbox("언어", lang_options, key="lang_f")
            with f3:
                text_mode = st.radio("리뷰 표시", ["번역", "원문"], horizontal=True, key="text_f")
            with f4:
                sort_order = st.radio("정렬", ["최신순", "오래된순"], horizontal=True, key="sort_f")

            df = reviews_df.copy()
            if vote_filter == "추천":
                df = df[df["voted_up"] == True]
            elif vote_filter == "비추천":
                df = df[df["voted_up"] == False]
            if lang_filter != "전체":
                df = df[df["language"] == lang_filter]
            df = df.sort_values("timestamp_created", ascending=(sort_order == "오래된순"))

            rows_html = ""
            for _, r in df.iterrows():
                ts = int(r["timestamp_created"]) if pd.notna(r["timestamp_created"]) else 0
                date_str = ts_to_kst_date(ts)
                vote = (
                    '<span style="color:#78a843;">추천</span>' if r["voted_up"]
                    else '<span style="color:#c0563f;">비추천</span>'
                )
                lang = r.get("language", "") or ""
                ptime_min = int(r["playtime_forever"]) if pd.notna(r["playtime_forever"]) else 0
                ptime = f"{ptime_min // 60}h"

                raw_text = (
                    r.get("review_text_ko") or r.get("review_text") or ""
                    if text_mode == "번역"
                    else r.get("review_text") or ""
                )
                if len(raw_text) > 200:
                    raw_text = raw_text[:200] + "..."
                text_safe = html.escape(raw_text)

                rows_html += (
                    f"<tr>"
                    f"<td style='white-space:nowrap;'>{date_str}</td>"
                    f"<td style='white-space:nowrap;'>{lang}</td>"
                    f"<td style='white-space:nowrap;'>{vote}</td>"
                    f"<td style='white-space:nowrap;'>{ptime}</td>"
                    f"<td style='color:#ccc;'>{text_safe}</td>"
                    f"</tr>"
                )

            render_scrollable_table(
                headers=[("날짜", 100), ("언어", 80), ("추천", 80), ("플레이", 80), ("리뷰", None)],
                rows_html=rows_html,
                height=500,
                extra_style=" margin-top:8px;",
            )

            st.caption(f"총 {len(df)}개 표시 (최근 300개 기준 필터)")

else:
    # ══════════════════════════════════════════════════════
    # Tab: Detail2 (가격 + 동접 분석)
    # ══════════════════════════════════════════════════════

    # ── 데이터 로드 (col_mid / col_right 공유) ────────────
    price_df = load_price_history(selected_app_id)
    peak_14d, low_14d = load_player_stats_14d(selected_app_id)

    if not price_df.empty:
        price_df["snapshot_date"] = pd.to_datetime(price_df["snapshot_date"])
        latest = price_df.sort_values("snapshot_date").iloc[-1]
        final_price = int(latest["final_price"])
        initial_price = int(latest["initial_price"])
        cur_discount = int(latest["discount_percent"])
        max_discount = int(price_df["discount_percent"].max())
    else:
        final_price, initial_price, cur_discount, max_discount = 0, 0, 0, 0

    if final_price > 0:
        price_html = f'<span style="color:#fff; font-size:20px; font-weight:bold;">{final_price:,}원</span>'
        if cur_discount > 0:
            price_html += f' <span style="color:#888; font-size:14px; text-decoration:line-through; margin-left:4px;">{initial_price:,}원</span>'
            price_html += f' <span style="background:#4c6b22; color:#fff; padding:2px 8px; border-radius:4px; font-size:13px; font-weight:bold; margin-left:8px;">-{cur_discount}%</span>'
    else:
        price_html = '<span style="color:#aaa; font-size:20px; font-weight:bold;">무료 / 정보없음</span>'

    max_disc_html = (
        f'<span style="color:#fff; font-size:20px; font-weight:bold;">-{max_discount}%</span>'
        if max_discount > 0 else '<span style="color:#aaa; font-size:20px; font-weight:bold;">할인 이력 없음</span>'
    )
    peak_html = (
        f'<span style="color:#fff; font-size:20px; font-weight:bold;">{peak_14d:,}명</span>'
        if peak_14d > 0 else '<span style="color:#aaa; font-size:20px; font-weight:bold;">데이터 없음</span>'
    )
    low_html = (
        f'<span style="color:#fff; font-size:20px; font-weight:bold;">{low_14d:,}명</span>'
        if peak_14d > 0 else '<span style="color:#aaa; font-size:20px; font-weight:bold;">데이터 없음</span>'
    )

    # ── 우측 영역: 카드 4개 (중앙 2 + 우측 2) + 히트맵 ────
    with col_content:
        st.markdown('<div style="margin-top:60px;"></div>', unsafe_allow_html=True)

        card_mid, card_right = st.columns(2, gap="large")
        with card_mid:
            st.markdown(f"""<div style="display:flex; gap:8px; height:110px;">
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px;">
<div style="color:#aaa; font-size:13px;">현재 가격</div>
<div style="margin-top:8px;">{price_html}</div>
</div>
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px;">
<div style="color:#aaa; font-size:13px;">최고 할인율</div>
<div style="margin-top:8px;">{max_disc_html}</div>
</div>
</div>""", unsafe_allow_html=True)
        with card_right:
            st.markdown(f"""<div style="display:flex; gap:8px; height:110px;">
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px;">
<div style="color:#aaa; font-size:13px;">최고 동시접속자 <span style="color:#666; font-size:11px;">(14일)</span></div>
<div style="margin-top:8px;">{peak_html}</div>
</div>
<div style="flex:1; background:#323E4D; border-radius:20px; padding:20px;">
<div style="color:#aaa; font-size:13px;">최저 동시접속자 <span style="color:#666; font-size:11px;">(14일)</span></div>
<div style="margin-top:8px;">{low_html}</div>
</div>
</div>""", unsafe_allow_html=True)

        # 카드 바로 아래 히트맵 (col_content 안이라 게임정보 높이와 무관하게 배치됨)
        st.subheader(" ")
        st.subheader("시간대별 동시접속자")

        heat_df = load_player_heatmap(selected_app_id, days=12)
        if heat_df.empty:
            st.info("동시접속자 데이터 없음")
        else:
            heat_df["day"] = pd.to_datetime(heat_df["day"])
            heat_df["hour_bin"] = heat_df["hour_bin"].astype(int)
            heat_df["player_count"] = heat_df["player_count"].astype(float)

            pivot = heat_df.pivot(index="day", columns="hour_bin", values="player_count")
            hour_bins = list(range(0, 24, 3))
            pivot = pivot.reindex(columns=hour_bins)
            pivot = pivot.sort_index(ascending=False)

            daily_total = heat_df.groupby("day")["player_count"].sum().astype(int)

            # 날짜별 할인율 + final_price 매핑
            # final_price는 할인 없으면 정가, 할인 있으면 할인가 (자동)
            # price_df에 없는 날짜는 ffill로 직전 값 유지
            discount_by_date = {}
            price_display_by_date = {}
            if not price_df.empty:
                pf = price_df.sort_values("snapshot_date").drop_duplicates("snapshot_date")
                full_range = pd.date_range(pf["snapshot_date"].min(), pf["snapshot_date"].max(), freq="D")
                pf = pf.set_index("snapshot_date").reindex(full_range).ffill().reset_index()
                pf.rename(columns={"index": "snapshot_date"}, inplace=True)
                for _, r in pf.iterrows():
                    d = pd.to_datetime(r["snapshot_date"]).date()
                    discount_by_date[d] = int(r["discount_percent"]) if pd.notna(r["discount_percent"]) else 0
                    price_display_by_date[d] = int(r["final_price"])
            discount_days = {d for d, v in discount_by_date.items() if v > 0}

            y_labels = []
            for d in pivot.index:
                d_date = d.date()
                total = daily_total.get(d, 0)
                base = f"{d.strftime('%m-%d')} ({int(total):,})"
                if d_date in discount_days:
                    y_labels.append(f"<b><span style='color:#16B2E2'>{base} 🏷</span></b>")
                else:
                    y_labels.append(base)

            x_labels = [f"{h:02d}시" for h in hour_bins]

            # hover용 customdata: [date_html, 할인율, 표시금액] per cell
            # 할인일이면 날짜를 파란색으로 스타일링 (plotly hovertemplate은 <span> 지원)
            # 표시금액: 할인일=할인가, 비할인일=정가
            customdata = []
            for d in pivot.index:
                d_date = d.date()
                date_str = d.strftime("%m-%d")
                pct = discount_by_date.get(d_date, 0)
                price_val = price_display_by_date.get(d_date, 0)
                date_html = (
                    f"<span style='color:#16B2E2'>{date_str}</span>"
                    if d_date in discount_days else date_str
                )
                customdata.append([[date_html, pct, price_val] for _ in hour_bins])

            fig_heat = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=x_labels,
                y=y_labels,
                customdata=customdata,
                colorscale=[[0, "#323E4D"], [1, "#b8d98a"]],
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>"
                    "할인율 %{customdata[1]}%% (%{customdata[2]:,}원)<br>"
                    "%{x} · 동접 %{z:,.0f}명"
                    "<extra></extra>"
                ),
                colorbar=dict(title="동접", thickness=12, tickfont=dict(size=14)),
            ))
            fig_heat.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc",
                margin=dict(t=20, b=10, l=0, r=10),
                height=520,
                xaxis=dict(title="", side="top", tickfont=dict(size=16)),
                yaxis=dict(title="", autorange="reversed", tickfont=dict(size=17)),
                hoverlabel=dict(font_size=15, font_family="sans-serif"),
            )
            st.plotly_chart(fig_heat, use_container_width=True)
            st.caption("🏷 = 할인 적용일 · 괄호 안 = 일 동접 합계 · 모든 시간은 KST 기준")


