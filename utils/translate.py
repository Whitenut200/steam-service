"""번역 공통 유틸리티 (무료 Google 웹 엔드포인트 사용)"""
from __future__ import annotations
from deep_translator import GoogleTranslator

_translator = None


def _get_translator():
    global _translator
    if _translator is None:
        _translator = GoogleTranslator(source="auto", target="ko")
    return _translator


def translate_to_ko(text: str, language: str = "") -> str | None:
    """번역 성공 시 한국어 반환, 실패 시 None (BQ에서 NULL로 남아 재시도됨)"""
    if not text or not text.strip():
        return None
    if language and language.lower() in ("korean", "koreana"):
        return text
    # deep-translator는 최대 5000자 제한 → 자르기
    snippet = text[:4900]
    # 일시적 네트워크 오류 대비 간단 재시도
    for attempt in range(3):
        try:
            result = _get_translator().translate(snippet)
            if result:
                return result
        except Exception as e:
            if attempt == 2:
                print(f"  번역 실패: {e}")
            else:
                import time
                time.sleep(1.5)
    return None
