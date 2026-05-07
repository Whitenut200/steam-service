"""Gemini Vertex AI 연결 테스트 — thinking ON/OFF 비교

사용법:
  python first/run_gemini_test.py

비교:
  - thinking ON (기본값) — 응답 전 내부 추론 → 품질 ↑, 비용 ↑
  - thinking OFF — 즉시 응답 → 단순 요약엔 충분, 비용 1/7 수준
"""
import sys
import os

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from utils.env import init_env
init_env()

from google import genai
from google.genai import types

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
LOCATION = "us-central1"
MODEL = "gemini-2.5-flash"

# 가격 (USD per 1M tokens) — Gemini 2.5 Flash 기준
PRICE_INPUT = 0.30
PRICE_OUTPUT = 2.50  # thinking 토큰도 출력 요금 적용


def estimate_cost(input_tokens: int, output_tokens: int, total_tokens: int) -> float:
    # total - input - output 차이는 thinking 토큰 (출력 요금 적용)
    thinking_tokens = max(0, total_tokens - input_tokens - output_tokens)
    cost = (
        input_tokens * PRICE_INPUT / 1_000_000
        + (output_tokens + thinking_tokens) * PRICE_OUTPUT / 1_000_000
    )
    return cost


def call_gemini(client, prompt: str, thinking: bool):
    config = None
    if not thinking:
        config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0)
        )
    return client.models.generate_content(model=MODEL, contents=prompt, config=config)


def print_result(label: str, response):
    usage = response.usage_metadata
    in_tok = usage.prompt_token_count
    out_tok = usage.candidates_token_count
    total = usage.total_token_count
    thinking = max(0, total - in_tok - out_tok)
    cost = estimate_cost(in_tok, out_tok, total)

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(response.text)
    print(f"\n토큰: 입력 {in_tok} | 출력 {out_tok} | thinking {thinking} | 합계 {total}")
    print(f"비용: ${cost:.6f} / 호출")
    print(f"  → 300게임: ${cost * 300:.2f}")


def main():
    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

    sample = {
        "긍정률": 72.3,
        "긍정_top_카테고리": ["재미/몰입", "스토리/세계관"],
        "긍정_키워드": ["협동", "재미", "그래픽"],
        "부정률": 27.7,
        "부정_top_카테고리": ["성능/서버"],
        "부정_키워드": ["서버", "렉", "핑"],
    }

    prompt = f"""다음 게임 리뷰 분석 결과를 그로스 마케터에게 보고하듯 친근한 한국어 한 문단으로 요약해줘.
통계 용어(p-value, 신뢰구간 등)는 쓰지 말고 의미와 액션 시그널 위주로 작성.
데이터에 없는 사실은 추측하지 마.

데이터:
{sample}
"""

    print(f"모델: {MODEL} | 리전: {LOCATION}")

    # thinking ON
    print("\n[1/2] thinking ON 호출 중...")
    resp_on = call_gemini(client, prompt, thinking=True)
    print_result("thinking ON", resp_on)

    # thinking OFF
    print("\n\n[2/2] thinking OFF 호출 중...")
    resp_off = call_gemini(client, prompt, thinking=False)
    print_result("thinking OFF", resp_off)

    # 비교 요약
    cost_on = estimate_cost(
        resp_on.usage_metadata.prompt_token_count,
        resp_on.usage_metadata.candidates_token_count,
        resp_on.usage_metadata.total_token_count,
    )
    cost_off = estimate_cost(
        resp_off.usage_metadata.prompt_token_count,
        resp_off.usage_metadata.candidates_token_count,
        resp_off.usage_metadata.total_token_count,
    )
    if cost_off > 0:
        ratio = cost_on / cost_off
        print(f"\n\n>>> thinking OFF가 {ratio:.1f}x 저렴 <<<")


if __name__ == "__main__":
    main()
