"""리뷰 텍스트 분석 — Kiwi 형태소 분석 기반

Cloud Function(analyze_daily)과 로컬 스크립트(first/run_analyze.py) 공용.
리뷰 문장을 감성 분류 후 카테고리 매칭 + 명사/형용사 키워드 빈도 집계.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from typing import Iterable

# 분류할 키워드 선택 
CATEGORIES = {
    "positive": {
        "재미/몰입": {
            "재미": ["재밌", "재미", "즐거", "꿀잼", "웃기", "신나"],
            "중독/몰입": ["중독", "몰입", "빠져들", "시간가는", "멈출수", "헤어나"],
            "리플레이": ["회차", "리플레이", "다시", "질리지", "또하", "반복해도"],
        },
        "스토리/세계관": {
            "스토리": ["스토리", "서사", "이야기", "전개", "결말", "시나리오"],
            "세계관/캐릭터": ["세계관", "캐릭터", "설정", "배경", "주인공", "NPC"],
            "자유도": ["자유", "선택지", "샌드박스", "모드", "커스텀", "오픈월드"],
        },
        "연출": {
            "그래픽": ["그래픽", "비주얼", "아트", "예쁘", "아름다", "미려", "디자인"],
            "사운드": ["음악", "OST", "사운드", "효과음", "음향", "배경음", "BGM"],
            "조작감": ["타격감", "손맛", "조작", "컨트롤", "부드러", "반응"],
        },
        "콘텐츠/가성비": {
            "콘텐츠": ["콘텐츠", "볼륨", "풍부", "다양", "할거", "엔드컨텐츠"],
            "가성비": ["가성비", "가격", "저렴", "세일", "합리적", "착한"],
            "멀티플레이": ["협동", "코옵", "멀티", "친구", "같이", "파티", "온라인"],
        },
    },
    "negative": {
        "버그/안정성": {
            "버그": ["버그", "오류", "글리치", "깨짐", "작동안", "고장"],
            "크래시": ["크래시", "튕김", "팅김", "강제종료", "꺼짐", "멈춤"],
            "핵/치트": ["핵", "치터", "어뷰징", "신고", "치트", "핵쟁이"],
        },
        "성능/서버": {
            "최적화": ["최적화", "프레임", "fps", "끊김", "로딩", "렉걸", "프레임드랍"],
            "서버": ["서버", "핑", "렉", "접속", "매칭", "대기", "디스커넥"],
        },
        "밸런스/난이도": {
            "밸런스": ["밸런스", "너프", "버프", "불균형", "사기", "편향", "밸패"],
            "난이도": ["난이도", "어렵", "불합리", "초보", "뉴비", "진입장벽"],
            "반복성": ["반복", "노가다", "지루", "단조", "뻔한", "질림", "루즈"],
        },
        "콘텐츠/과금": {
            "콘텐츠부족": ["부족", "짧음", "적음", "아쉬", "금방", "얼마안"],
            "과금": ["과금", "DLC", "유료", "결제", "비싸", "돈", "현질"],
            "UI/UX": ["UI", "인터페이스", "불편", "직관", "번역", "메뉴", "조잡"],
        },
    },
}

NEGATIVE_MARKERS = ["않", "안", "못", "없", "싫", "별로", "아쉬", "부족", "최악", "쓰레기",
                     "노잼", "후회", "환불", "비추", "실망", "짜증", "불편", "거지", "똥", "망",
                     "재미없", "노답", "발적화", "버려진", "시간낭비", "돈낭비", "망겜", "쓰래기",
                     "구리", "구려", "최하", "에바", "거른다", "걸러", "사지마", "사기마", "취소",
                     "지루", "심심", "허접", "조잡"]
POSITIVE_MARKERS = ["좋", "훌륭", "최고", "추천", "재밌", "만족", "완벽", "명작", "갓", "꿀잼",
                     "감동", "대박", "사세요", "강추", "즐거", "행복", "예쁘", "멋지",
                     "재미있", "재밋", "잼있", "꿀겜", "갓겜", "인생겜", "띵작", "수작", "수준급",
                     "쩐다", "쩔어", "개꿀", "혜자", "꼭사", "사라", "굿겜", "갓성비", "혜자겜",
                     "재밌어", "재미있어", "좋아", "좋았", "추천해"]

STOPWORDS = {
    "진짜", "정말", "너무", "그냥", "완전", "매우", "조금", "약간", "제발", "솔직히",
    "그리고", "근데", "그래서", "또", "좀", "이거", "저거", "이것", "저것", "여기",
    "게임", "플레이", "시간", "사람", "정도", "생각", "느낌", "부분", "경우", "이상",
    "처음", "다음", "하나", "그것", "자체", "모습", "상태", "이후", "이전", "전체",
}


# Kiwi는 지연 초기화 (Cloud Function cold start 시 모듈 로드만 되고 인스턴스는 실제 사용 시점에 생성)
_kiwi = None

def _get_kiwi():
    global _kiwi
    if _kiwi is None:
        from kiwipiepy import Kiwi
        _kiwi = Kiwi()
    return _kiwi


NEGATION_SCOPE = {"안", "않", "못", "별로", "전혀", "하나도", "결코"}


def analyze_sentence_sentiment(sentence: str) -> str:
    """문장 감성 판단: positive / negative / neutral"""
    kiwi = _get_kiwi()
    tokens = kiwi.tokenize(sentence)
    forms = [t.form for t in tokens]
    text = sentence.lower()

    pos_count = sum(1 for m in POSITIVE_MARKERS if m in text)
    neg_count = sum(1 for m in NEGATIVE_MARKERS if m in text)

    # 부정 스코프: 긍정 마커 토큰 앞 2토큰 안에 부정 토큰이 있으면 긍정 → 부정 뒤집기
    # 예: "안 재밌다", "별로 재미있지", "전혀 추천 안함"
    for i, t in enumerate(tokens):
        if not any(m in t.form for m in POSITIVE_MARKERS):
            continue
        window = forms[max(0, i - 2):i]
        if any(w in NEGATION_SCOPE for w in window):
            pos_count -= 1
            neg_count += 1

    for t in tokens:
        if t.tag == "VCN":
            neg_count += 1
        if t.form in ("않", "못", "안") and t.tag.startswith("VX"):
            neg_count += 1

    if neg_count > pos_count:
        return "negative"
    if pos_count > neg_count:
        return "positive"
    return "neutral"


def filter_sentences(review_text: str, voted_up: bool) -> list[str]:
    """2차 감성분석: 리뷰 문장 중 감성에 맞는 문장만 반환"""
    kiwi = _get_kiwi()
    sentences = kiwi.split_into_sents(review_text)
    filtered = []
    for sent in sentences:
        text = sent.text.strip()
        if len(text) < 5:
            continue
        sentiment = analyze_sentence_sentiment(text)
        if voted_up:
            if sentiment != "negative":
                filtered.append(text)
        else:
            # 비추천 리뷰는 negative 문장만 담음 (neutral 제외 → 노이즈 키워드 차단)
            if sentiment == "negative":
                filtered.append(text)
    return filtered


def match_categories(sentences: Iterable[str], polarity: str) -> dict:
    """문장들에서 카테고리 키워드 매칭 → {(상위, 하위): count}"""
    cats = CATEGORIES.get(polarity, {})
    result = defaultdict(int)
    combined = " ".join(sentences).lower()
    for top_cat, subcats in cats.items():
        for sub_cat, keywords in subcats.items():
            for kw in keywords:
                if kw.lower() in combined:
                    result[(top_cat, sub_cat)] += combined.count(kw.lower())
    return dict(result)


def extract_keywords(sentences: Iterable[str], polarity: str | None = None) -> Counter:
    """문장들에서 명사(NNG/NNP) 키워드 추출
    polarity="positive"면 NEGATIVE_MARKERS를, "negative"면 POSITIVE_MARKERS를 키워드에서 제외
    (반대 극성 단어가 키워드로 잡히는 노이즈 차단)
    """
    kiwi = _get_kiwi()
    if polarity == "positive":
        blacklist = NEGATIVE_MARKERS
    elif polarity == "negative":
        blacklist = POSITIVE_MARKERS
    else:
        blacklist = []

    keywords: Counter = Counter()
    for sent in sentences:
        for t in kiwi.tokenize(sent):
            if len(t.form) < 2 or t.form in STOPWORDS:
                continue
            if any(b in t.form for b in blacklist):
                continue
            if t.tag in ("NNG", "NNP"):
                keywords[(t.form, t.tag)] += 1
    return keywords


def analyze_reviews(rows: list) -> dict:
    """리뷰 목록을 받아 한 게임 분석 결과 dict 반환

    rows: [{"voted_up": bool, "review_text_ko": str}, ...] 또는 BQ Row 객체
    """
    pos_sentences = []
    neg_sentences = []
    total_pos = 0
    total_neg = 0

    for row in rows:
        voted_up = row["voted_up"] if isinstance(row, dict) else row.voted_up
        text = row["review_text_ko"] if isinstance(row, dict) else row.review_text_ko
        if not text:
            continue
        filtered = filter_sentences(text, voted_up)
        if voted_up:
            pos_sentences.extend(filtered)
            total_pos += 1
        else:
            neg_sentences.extend(filtered)
            total_neg += 1

    return {
        "total": len(rows),
        "total_positive": total_pos,
        "total_negative": total_neg,
        "pos_sentences": len(pos_sentences),
        "neg_sentences": len(neg_sentences),
        "pos_categories": match_categories(pos_sentences, "positive"),
        "neg_categories": match_categories(neg_sentences, "negative"),
        "pos_keywords": extract_keywords(pos_sentences, "positive").most_common(30),
        "neg_keywords": extract_keywords(neg_sentences, "negative").most_common(30),
    }
