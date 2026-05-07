"""Gemini 자연어 코멘트 생성 (Vertex AI)

분석 결과 dict를 받아 그로스 마케팅 톤의 한국어 코멘트로 변환.
실패 시 템플릿 fallback.
"""
import os

LOCATION = "us-central1"
MODEL = "gemini-2.5-flash"

SECTION1_PROMPT = """너는 게임 리뷰 분석 결과를 그로스 마케터에게 보고하는 어시스턴트야.

데이터 구성:
- 긍정률/부정률, 총 리뷰수
- 긍정_top_카테고리 / 부정_top_카테고리: 각 항목은 {name, count, share_pct} — share_pct는 그 polarity 안에서의 점유율 (예: 부정 의견 중 35.2%가 '최적화')
- 차별화_키워드_전체: 전체 게임 대비 이 게임에서만 두드러지는 키워드 (Top 3)
- 차별화_키워드_장르: 같은 장르 게임 대비 두드러지는 키워드 (None이면 해당 장르 게임이 적어 비교 불가)
- 긍정률_4주추세: 최근 4주 주별 긍정률 + "최근주 vs 직전3주 평균" 변화 (None이면 누적 데이터 부족)

작성 규칙:
- 친근한 한국어로 한 문단 (4~5줄)
- 통계 용어(TF-IDF, p-value, 코퍼스 등) 사용 금지 — 일반인 표현으로
- 부정 카테고리 언급할 땐 share_pct를 반드시 인용해 구체화: "최적화는 부정 의견 중 35%를 차지해 운영 우선순위가 가장 높아 보입니다" 식
- 긍정 카테고리도 가능하면 share_pct를 함께 (선택적)
- 차별화 키워드는 "전체 게임 대비"인지 "같은 장르 게임 대비"인지 반드시 구분해서 표현:
  * 전체/장르 키워드가 다르면: "전체 게임 대비 X가 두드러지고, 같은 장르 안에선 Y가 차별점"
  * 같으면: "전체와 같은 장르 양쪽에서 X가 두드러져 더 강력한 마케팅 메시지로 활용 가능"
  * 장르가 None이면: "전체 게임 대비"로만 표현, 장르 비교는 언급 금지
- 추세는 변화_pp 기준 ±2pp 이상일 때 의미있게 언급, 미만이면 생략 또는 "최근 추세는 안정적" 정도
- 추세가 None이면 추세에 관한 어떤 문장도 쓰지 마. "데이터가 부족하다" / "확인되지 않습니다" / "파악하기 어렵습니다" 같은 메타 코멘트도 절대 금지. 추세는 통째로 없는 항목으로 간주
- 데이터에 없는 사실은 절대 추측·생성 금지
- 마크다운 헤더(#)나 글머리 기호(-) 사용 금지 — 평문 한 문단으로
"""


SECTION2_PROMPT = """너는 게임 리뷰 분석 결과를 그로스 마케터에게 보고하는 어시스턴트야.
이번 섹션은 "유저 세그먼트 시그널" — 어떤 유저층이 만족/불만인지 구조적으로 살펴보는 자리야.

데이터 구성:
- 플레이타임_코호트: 0-10h / 10-50h / 50h+ 버킷별 추천률(pos_ratio)과 표본수(n) + 전체 평균
  * 코호트 간 추천률 차이가 진입장벽/이탈 시그널을 알려줌
  * None이면 표본 부족으로 분석 불가
- 언어_갭: Top 5 언어별 추천률(pos_ratio) + 최고/최저/갭_pp
  * 특정 언어 사용자의 만족도 차이는 현지화/시장진입 의사결정 시그널
  * 언어 코드는 영어 표기(예: schinese=중국어 간체, brazilian=브라질 포르투갈어)이므로 한국어 명칭으로 풀어 표현
  * None이면 표본 부족 또는 언어 다양성 부족으로 분석 불가

작성 규칙:
- 친근한 한국어로 한 문단 (3~4줄)
- 통계 용어 금지 — "표본", "유의성" 같은 단어 사용 금지
- 코호트 분석 톤: "10시간 미만 신규 유저는 추천률 X%로 진입장벽이 보이는 반면, 50시간 이상 헤비유저는 Y%로 안정적" 식으로 구체적 수치 인용
  * 코호트 간 차이가 5pp 미만이면 "고른 만족도" 정도로 짧게, 5pp 이상이면 의미있게 해석
- 언어 갭 톤: "최저 언어가 X%로 평균 대비 낮음 → 현지화 우선순위" 또는 "최고 언어 Y%는 해당 시장에서 검증된 호응 → 마케팅 집중 시그널"
  * 갭_pp가 5pp 미만이면 "언어별 만족도가 고르게 분포" 정도, 5pp 이상이면 구체 언어 인용
- 코호트나 언어_갭이 None이면 그 항목은 통째로 언급 금지 ("데이터 부족" 같은 메타 코멘트도 금지)
- 데이터에 없는 사실은 절대 추측·생성 금지
- 마크다운 헤더(#)나 글머리 기호(-) 사용 금지 — 평문 한 문단으로
"""


SECTION3_PROMPT = """너는 게임 리뷰 분석 결과를 그로스 마케터에게 보고하는 어시스턴트야.
이번 섹션은 "변화 알림" — 최근 일어난 이벤트(패치/세일/키워드 변화/동접 이상치)와 그 효과를 짚어주는 자리야.

데이터 구성:
- 패치_임팩트: 최근 1건의 뉴스(이벤트_제목/일자) ±7일 동안의 추천률·동접 변화. None이면 최근 60일 내 뉴스 부재.
  * 추천률_변화 안에 before_n / after_n 표본수가 있는데, after_n < 30이면 "표본이 작다"는 점을 함께 언급
  * 추천률_변화.유의성: 두 비율 차이 통계 검정 결과 — 뚜렷한_상승 / 약한_상승 / 유의미한_변화_없음 / 약한_하락 / 뚜렷한_하락
  * 동접_변화.유의성: 비-패치 baseline 동접 변동 기준 — 뚜렷한_증가 / 약한_증가 / 평소_변동_범위 / 약한_감소 / 뚜렷한_감소
- 세일_효과: 최근 가장 큰 할인 윈도(세일_시작~세일_종료, 할인율) 동안의 동접
  * 변화율_pct: 세일 직전 7일 대비 단순 % 변화 (맥락용)
  * z_score / 유의성: 비-세일 baseline 동접의 변동성을 기준으로 한 통계적 유의성 — 뚜렷한_증가 / 약한_증가 / 평소_변동_범위 / 약한_감소 / 뚜렷한_감소
  * z_score=None이면 baseline 데이터 부족 → 변화율_pct만 가지고 단순 서술
  * None이면 최근 60일 세일 없음
- 부정_키워드_급증: "급증"(직전 4주 평균 대비 2배 이상), "신규"(이전엔 없던 단어). None이면 의미있는 변화 없음
- 동접_이상치: 최근 7일 중 z-score |2| 이상 일자. 각 이상치엔 ±2일 내 근처_뉴스 / 근처_세일 cross-ref 동봉. None이면 평소와 비슷

작성 규칙:
- 친근한 한국어로 한 문단 (4~5줄)
- 통계 용어(z-score, 표준편차) 금지 — "평소보다 ##% 많은 동접"식 일반인 표현
- 이벤트 서사 톤: "최근 X에서 Y가 일어났고, 그 결과 Z 시그널"
- 패치 임팩트 (서술 패턴: 구체 숫자 → 통계 검정 결론 → 액션 시그널):
  * 항상 절대 수치(delta_pp / delta_pct)를 먼저 짚고, 그 다음 "통계적 검정 결과 유의미한/유의미하지 않은 변화로 나왔습니다" 식으로 결론을 평이하게 덧붙이고, 마지막에 "따라서 ~" 형태로 액션 시그널 마무리
  * 추천률_변화.유의성=뚜렷한_상승: 예) "추천률이 12.9%p 상승했고 통계적 검정 결과 유의미한 변화로 나왔습니다. 따라서 패치 방향성이 호평받았다고 볼 수 있습니다"
  * 추천률_변화.유의성=약한_상승/하락: 예) "추천률이 5%p 올랐고 약한 상승 신호로 잡혔습니다. 추세는 며칠 더 봐야 안전합니다"
  * 추천률_변화.유의성=유의미한_변화_없음: 예) "추천률이 3.6%p 떨어졌지만 표본 차이를 감안하면 통계적으로 의미있는 변화로 보긴 어렵습니다. 따라서 추천률은 패치 영향을 받지 않은 것으로 해석됩니다"
  * 동접_변화.유의성=뚜렷한_증가/감소: 예) "동접이 18% 늘었고 통계적 검정 결과 평소 수준을 뚜렷이 넘어선 변화입니다. 따라서 패치가 사람들을 끌어모은 효과로 볼 수 있습니다"
  * 동접_변화.유의성=약한_증가/감소: 예) "동접이 8% 늘었고 약한 증가 정도의 신호로 보입니다"
  * 동접_변화.유의성=평소_변동_범위: 예) "동접이 5% 줄었지만 통계적으로는 평소 변동 범위 안이라 패치 영향으로 단정하기 어렵습니다"
  * after_n < 30 시 반드시 "패치 직후라 표본이 적어 추세는 더 봐야 함" 명시
  * 허용 통계 표현: "통계적 검정 결과", "통계적으로 의미있는/유의미한 변화", "평소 변동 범위 안", "약한 증가/감소", "뚜렷한 증가/감소"
  * 금지 용어: "z-score", "표준편차", "p-value", "신뢰구간", "분산", "정규분포" 등 raw 통계 용어
- 세일 효과 (서술 패턴: 구체 숫자 → 통계 검정 결론 → 액션 시그널):
  * 항상 변화율_pct를 먼저 짚고, 그 다음 유의성을 평이하게 풀고, 마지막에 "따라서 ~" 액션 시그널
  * 유의성=뚜렷한_증가: 예) "60% 할인 동안 동접이 직전 대비 22% 늘었고 통계적 검정 결과 평소 변동을 뚜렷이 넘어선 변화로 나왔습니다. 따라서 가격 정책이 명확히 통한 신호로 볼 수 있습니다"
  * 유의성=약한_증가: 예) "할인 기간 동접이 12% 늘었고 약한 증가 신호로 잡혔습니다. 효과는 있었지만 강하다고 보긴 어렵습니다"
  * 유의성=평소_변동_범위: 예) "할인 기간 동접이 22% 늘었지만 통계적으로는 평소 변동 범위 안이라 세일 효과로 단정하긴 어렵습니다. 따라서 가격 인하 자체보다 다른 요인 영향일 가능성도 봐야 합니다"
  * 유의성=약한_감소/뚜렷한_감소: 예) "할인에도 동접이 5% 줄었고 통계적으로도 의미있는 감소로 잡혔습니다. 따라서 가격 인하만으로 관심을 회복하기엔 한계가 있다는 신호입니다"
  * z_score=None (베이스라인 부족) 시: 변화율_pct만으로 짧게 서술하되 단정 톤 피하기 ("아직 누적 데이터가 적어 효과를 단정하긴 어렵습니다" 식)
  * 허용/금지 통계 표현은 패치 임팩트 룰과 동일
- 부정 키워드 급증:
  * 신규 키워드는 그대로 인용하며 "이전엔 없던 단어가 새로 등장 → 새로운 불만 발생 시그널"
  * 급증 키워드는 ratio를 인용하며 "X가 평소 대비 N배 늘어남"
  * 운영 우선순위로 검토 권고
- 동접 이상치:
  * 각 이상치 일자의 근처_뉴스/근처_세일이 있으면 "X일 동접 급증/급락은 [패치 또는 세일] 영향으로 보임"
  * cross-ref 매칭이 없으면 "원인 미상, 외부 요인 가능성"
- 항목이 None이면 그 항목은 통째로 언급 금지 ("변화 없음" 같은 메타 코멘트도 금지)
- 데이터에 없는 사실은 절대 추측·생성 금지
- 마크다운 헤더(#)나 글머리 기호(-) 사용 금지 — 평문 한 문단으로
"""


_client = None


def _get_client():
    global _client
    if _client is None:
        from google import genai
        project = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
        _client = genai.Client(vertexai=True, project=project, location=LOCATION)
    return _client


def _generate(prompt_header: str, insights: dict) -> str | None:
    """Gemini 호출 공통. 실패 시 None."""
    try:
        from google.genai import types
        client = _get_client()
        prompt = f"{prompt_header}\n\n분석 데이터:\n{insights}"
        config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0)
        )
        response = client.models.generate_content(
            model=MODEL, contents=prompt, config=config
        )
        text = (response.text or "").strip()
        return text or None
    except Exception:
        return None


def narrate_sentiment(insights: dict) -> str:
    """섹션 ① 자연어 코멘트. 실패 시 템플릿 fallback."""
    return _generate(SECTION1_PROMPT, insights) or template_sentiment(insights)


def narrate_segment(signals: dict) -> str:
    """섹션 ② 자연어 코멘트. 실패 시 템플릿 fallback."""
    return _generate(SECTION2_PROMPT, signals) or template_segment(signals)


def narrate_events(signals: dict) -> str:
    """섹션 ③ 자연어 코멘트. 실패 시 템플릿 fallback."""
    return _generate(SECTION3_PROMPT, signals) or template_events(signals)


def template_sentiment(insights: dict, error: str = "") -> str:
    """API 실패 시 fallback — f-string 기반 평문 narrative"""
    pos = insights.get("긍정률", 0)
    neg = insights.get("부정률", 0)
    pos_cat = ", ".join(
        f"{c['name']}({c['share_pct']}%)" for c in insights.get("긍정_top_카테고리", [])[:2]
    ) or "—"
    neg_cat = ", ".join(
        f"{c['name']}({c['share_pct']}%)" for c in insights.get("부정_top_카테고리", [])[:2]
    ) or "—"

    overall = insights.get("차별화_키워드_전체") or {}
    diff_pos = ", ".join(k["keyword"] for k in overall.get("긍정", [])[:3]) or "—"
    diff_neg = ", ".join(k["keyword"] for k in overall.get("부정", [])[:3]) or "—"

    genre = insights.get("차별화_키워드_장르")
    genre_line = ""
    if genre:
        gp = ", ".join(k["keyword"] for k in genre.get("긍정", [])[:3]) or "—"
        gn = ", ".join(k["keyword"] for k in genre.get("부정", [])[:3]) or "—"
        genre_line = f"\n같은 장르({genre.get('장르', '')}) 비교 — 긍정 {gp} / 부정 {gn}"

    trend = insights.get("긍정률_4주추세")
    trend_line = ""
    if trend and trend.get("변화_pp") is not None:
        d = trend["변화_pp"]
        trend_line = (
            f"\n최근주 긍정률 {trend['최근주_긍정률']}% "
            f"(직전 3주 평균 대비 {'+' if d >= 0 else ''}{d}pp)"
        )

    err_note = f" (AI 응답 실패: {error[:60]})" if error else ""
    return (
        f"긍정 {pos}% / 부정 {neg}%\n"
        f"긍정 측면: {pos_cat} | 부정 측면: {neg_cat}\n"
        f"이 게임만의 특징 — 긍정 {diff_pos} / 부정 {diff_neg}"
        f"{genre_line}{trend_line}{err_note}"
    )


def template_segment(signals: dict, error: str = "") -> str:
    """섹션 ② fallback — 코호트/언어 갭 평문"""
    parts = []
    cohort = signals.get("플레이타임_코호트")
    if cohort and cohort.get("버킷별"):
        bucket_str = " / ".join(
            f"{b['bucket']} {b['pos_ratio']}%(n={b['n']})" for b in cohort["버킷별"]
        )
        parts.append(f"플레이타임 코호트: {bucket_str}")

    lang = signals.get("언어_갭")
    if lang and lang.get("언어별"):
        lang_str = " / ".join(
            f"{l['language']} {l['pos_ratio']}%(n={l['n']})" for l in lang["언어별"][:5]
        )
        parts.append(f"언어별: {lang_str} (갭 {lang['갭_pp']}pp)")

    if not parts:
        return "세그먼트 분석 데이터 부족"

    err_note = f" (AI 응답 실패: {error[:60]})" if error else ""
    return "\n".join(parts) + err_note


def template_events(signals: dict, error: str = "") -> str:
    """섹션 ③ fallback — 이벤트 평문 요약"""
    parts = []
    patch = signals.get("패치_임팩트")
    if patch:
        rd = patch.get("추천률_변화") or {}
        pd = patch.get("동접_변화") or {}
        line = f"패치 '{patch['이벤트_제목']}' ({patch['이벤트_일자']})"
        if rd:
            line += f" → 추천률 {rd['before_pos_ratio']}% → {rd['after_pos_ratio']}% ({rd['delta_pp']:+}pp"
            if rd.get("유의성"):
                line += f", {rd['유의성'].replace('_', ' ')}"
            line += ")"
        if pd and pd.get("delta_pct") is not None:
            line += f", 동접 {pd['delta_pct']:+}%"
            if pd.get("유의성"):
                line += f" ({pd['유의성'].replace('_', ' ')})"
        parts.append(line)

    sale = signals.get("세일_효과")
    if sale:
        ch = sale.get("변화율_pct")
        sig = sale.get("유의성")
        z = sale.get("z_score")
        line = f"세일 {sale['할인율']}% ({sale['세일_시작']}~{sale['세일_종료']})"
        if ch is not None:
            line += f" → 동접 {ch:+}%"
        if sig:
            line += f" [{sig.replace('_', ' ')}, z={z}]"
        parts.append(line)

    surge = signals.get("부정_키워드_급증")
    if surge:
        if surge.get("급증"):
            sg = ", ".join(f"{k['keyword']}(×{k['ratio']})" for k in surge["급증"][:3])
            parts.append(f"부정 키워드 급증: {sg}")
        if surge.get("신규"):
            ng = ", ".join(k["keyword"] for k in surge["신규"][:3])
            parts.append(f"신규 부정 키워드: {ng}")

    anom = signals.get("동접_이상치")
    if anom:
        for a in anom["이상치"][:3]:
            ctx = []
            if a.get("근처_뉴스"):
                ctx.append(f"뉴스: {a['근처_뉴스'][0]['title'][:30]}")
            if a.get("근처_세일"):
                ctx.append(f"세일 {a['근처_세일'][0]['할인율']}%")
            parts.append(
                f"{a['date']} 동접 {a['avg_players']:,} (z={a['z_score']})"
                + (f" — {' / '.join(ctx)}" if ctx else "")
            )

    if not parts:
        return "최근 이벤트 시그널 없음"

    err_note = f" (AI 응답 실패: {error[:60]})" if error else ""
    return "\n".join(parts) + err_note
