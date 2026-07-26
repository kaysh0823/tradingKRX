# tradingKRX 정합성 진단 리포트

- **기준 정본:** `.cursor/rules/project-structure.mdc` (+ `krx-data-fetch.mdc`)
- **점검일:** 2026-07-26
- **범위:** 리포지토리 전체 (코드 수정 없음, 파일 열람·검색 기반)
- **표기:** 확인 못한 항목은 **미확인**

---

## 1. 요약

### 정합성 점수: **52 / 100**

| 영역 | 점수(대략) | 한줄 평가 |
|------|-----------|----------|
| 폴더 역할 | 70 | 대골격은 맞으나 `00.*`·구버전·pykrx 잔존 |
| 지표 로직 정합 | 40 | 동명 지표의 수식·기간·파라미터가 폴더별 분기 |
| 데이터 원천 | 55 | naverPub·최신 OTP 경로는 양호, 로컬 3파일 pykrx 위반 |
| 저장소 분리 | 35 | 중첩 git + 루트가 naverPub 파일을 함께 추적 |
| 보안·품질 | 30 | 로컬 스크립트 시크릿 하드코딩 다수 |

### 주요 리스크 Top5

| # | 심각도 | 리스크 |
|---|--------|--------|
| 1 | **High** | 로컬 10~50/`00.*`에 DB·KRX·KIS 시크릿 하드코딩 (값은 아래 마스킹) |
| 2 | **High** | 루트 git이 `naverPub/*` 약 30파일을 추적 + `naverPub/.git` 중첩 → 이중 커밋·드리프트 |
| 3 | **High** | 공통 지표 정의 분기: 에너지(tanh K=15 유무), Talent(시가→종가 일수 vs 종가등락 지수), RS 평균 구간, ATR(Wilder vs SMA) |
| 4 | **Med** | `pykrx` 활성 import 3파일 — 규칙 명시 금지 |
| 5 | **Med** | ~~버전 스크립트 난립 + 규칙 문서 http/https 불일치~~ → **해결** (`archive/` 이관, `https://data.krx.co.kr` 통일, 2026-07-26) |

---

## 2. 폴더별 현황표

| 폴더 | 역할일치 | 주요파일 | 이슈수(대략) | 비고 |
|------|----------|----------|-------------|------|
| `10. OHLCV` | 대체로 일치 | `11. KRX_ohlcv_v4.0.py` | 4 | 수집·RS 적재 역할 OK. `pykrx` import, 시크릿 하드코딩 |
| `20. MARKET Analysis` | 일치 | `21. KRX_market_analysis_v2.0.py` | 3 | breadth/CVI/ADR 정본에 가깝지만 DB URL·시크릿, ATR=talib |
| `30. ETF` | 대체로 일치 | `31…v6`, `32…v2`, `33…GRAPH` | 4 | PDF/시세 역할 OK. v1은 `archive/`. `31` pykrx 잔존 |
| `40. Trading` | 일치 | `41. tradingKIS_v3.0.py` | 3 | 보유종목·KIS 역할 OK. “3일에너지” 정의 상이, API키 하드코딩 |
| `50. Picking` | 일치 | `51. Picking_KRX_v4.0.py` | 3 | 스크리닝 정본. picking 가중점수 없음(naverPub 전용) |
| `naverPub` | 일치 | `runner/collect/content_*/screening/krx_client` | 6 | 발행·VPS 역할 OK. 지표 이식 시 분기·루트와 이중 추적 |
| 루트 `00.*` | **구조 밖** | `00. krx_info_v3.0.py` (v2→`archive/`) | 3 | OTP https 정본. 시크릿·구조 분류는 잔여 |

**역할 이탈 사례**

- 수집/`00`/`10`에 발행(naverPub) 로직은 없음 → 역할 혼선은 제한적.
- `naverPub/collect_daily.py`가 OHLCV·PDF·RS·talent까지 수집 — 규칙상 “발행용 재구성”으로 허용되나, **로컬 10과 이중 수집 파이프**가 존재.

**버전 난립** — **해결 (2026-07-26)**

| 위치 | 활성(정본) | 구버전 |
|------|------------|--------|
| 루트 | `00. krx_info_v3.0.py` | `archive/00. krx_info_v2.0.py` (git 추적 유지) |
| 30. ETF | `31…v6`, `32…v2`, `33…GRAPH` | `archive/32. ETF_PDF_v1.0.py` |
| 기타 | `11…v4` / `21…v2` / `41…v3` / `51…v4` | 폴더당 1개 — 병존 없음 |

방침: `archive/`는 **git 추적 유지** (삭제·gitignore 제외 안 함). 활성 코드에서 archive 경로 참조 금지. 상세 `archive/README.md`.
KRX URL: data.krx.co.kr 는 **https** 만 사용 (문서·주석·규칙 통일 완료, `.cursor/rules/krx-data-fetch.mdc`).

---

## 3. 공통 지표 정합성 매핑표

| 지표 | 정의 위치(파일:라인) | 폴더별 일치 | 불일치 상세 |
|------|----------------------|-------------|-------------|
| **RS 산출** | `10…/11. KRX_ohlcv_v4.0.py` (~3568+); `naverPub/collect_daily.py:22,736-773` | **대체로 일치** | periods=`[10,20,50,120,200]`, `ffill(limit=20)`, 상대수익률 백분위 — 양쪽 동일 계열 |
| **RS 순위/평균** | `20…/21…:1496-1511` (`rs_10~120` 평균); `naverPub/content_market.py:583,622-623` (`rs_20~200` 평균) | **불일치** | 20은 10일 포함·200 제외; naverPub Top50은 10 제외·200 포함 |
| **에너지배율 기본** | `20…/21…:737-746` 등 `tv%/mcap%`; `naverPub/content_market.py:343-370`; `50…/51…`; `40…:1339-1350` | **기본식 일치** | 분자·분모 비중비는 공유 |
| **에너지 방향가중** | `naverPub/content_market.py:26-27,372-403` `ENERGY_DIR_K=15`, `×(1+tanh(ret%/K))` | **불일치** | 규칙이 K=15를 공통으로 명시하나 **tanh는 naverPub에만** 존재. 20/40/50은 raw |
| **3일 에너지** | `20`/`naverPub`: 3일 TV합 비중÷시총비중; `40…:1805-1807` `np.mean(ers[:3])` | **불일치** | Trading은 일별 ER 평균 ≠ 3일 대금합 비율 |
| **Talent** | `20…/21…:1136-1149` 시가→종가 ≥+10% **일수**; `naverPub/collect_daily.py:23-24,896+` 동일 계열; `naverPub/content_market.py:566-574,696,787` 종가등락 ±10% + `(n20/20)*0.5+…` **지수** | **중대 불일치** | 이벤트 정의(시가대비 vs 전일대비)와 단위(일수 vs 가중지수)가 갈라짐. `50`은 open→close를 **비율%**로도 표현 |
| **주가위치** | `naverPub/content_market.py:35-37,847-961` (120 HL + 20/50 종가) | **naverPub 전용** | 20/10/40에는 동명 지표 없음. 스크리닝의 consol position(`screening.py:563+`)은 별개 |
| **ATR14/종가** | `20`/`50`: talib Wilder; `naverPub/content_volatility.py:204-216` **SMA(TR,14)**; `30…/31…` SMA 계열 | **불일치** | 창 14·/종가는 공유, 스무딩(Wilder vs SMA) 상이. 시장집계: 20 등가중 vs naverPub 시총가중 |
| **모멘텀 속도** | `20…:3554+` periods `(5,10,20,50)`; `naverPub/content_volatility.py:32,256-265` `(20,50)` | **식 일치·기간 부분일치** | `ROC%/N` 동일, 표시 기간만 축소 |
| **CVI / ADR** | `20…/21…:4355-4446`; `naverPub/content_volatility.py:456-490` | **대체로 일치** | 20일 roll, ADR×100, CVI=상승TV합/하락TV합. TV 소스: 20은 `close*volume` 근사, naverPub은 `trading_value` 컬럼 |
| **스크리닝** | `50…/51…` ↔ `naverPub/screening.py` (이식) | **대체로 일치** | 시총 2,000억, ATR gate 등. 발행 차트 `min_patterns=3`은 naverPub 게이트 |
| **picking 점수** | `naverPub/content_picking.py:22-50` | **naverPub 전용** | 순위→250~50점 + long/short 가중. `50.Picking`에 동등 구현 없음 |

### 중복 재구현 목록

| 로직 | 위치들 |
|------|--------|
| RS 벡터 엔진 | `10…/11…` ≈ `naverPub/collect_daily.py` |
| 에너지 tv%/mcap% | `20`, `40._energy_ratio`, `50`, `content_market`(+tanh) |
| 스크리닝 루프 | `51.Picking_KRX_v4.0` ↔ `naverPub/screening.py` |
| 모멘텀 속도 ROC/N | `21` ↔ `content_volatility` |
| CVI/ADR breadth | `21` ↔ `content_volatility._compute_breadth` |
| KRX 로그인/OTP | `00.v3`, `10`, `32.v1/v2`, `naverPub/krx_client.py` |
| Talent open→close | `21._talent_days_from_ohlcv`, `collect_daily`, (`50` %버전) |

---

## 4. 위반·중복·죽은코드 목록

| 파일:라인 | 유형 | 설명 | 권고 |
|-----------|------|------|------|
| `30.ETF/31.ETF_ohlcv_v6.0.py:40` (+사용처) | 규칙위반(pykrx) | `from pykrx import stock` 실사용 | OTP/`krx_client` 패턴으로 교체 |
| `10.OHLCV/11.KRX_ohlcv_v4.0.py:31` | 규칙위반(pykrx) | import 활성(호출 다수 주석) | import 제거·잔여 호출 점검 |
| `00.krx_info_v2.0.py:19` | 규칙위반(pykrx) | 영업일 판별에 실사용 | v3 경로로 통합 후 v2 폐기 |
| `00.krx_info_v3.0.py:28` 등 | 보안 | `os.environ['KRX_PW']='***'` 하드코딩 | getenv만, `.env` |
| `10…/11…:37`, `503`, `781` 등 | 보안 | KRX_PW·MySQL `passwd='***'` 다수 | 동일 |
| `30…/32.ETF_PDF_v1.0.py:132-133`, `v2:246-247` | 보안 | uid/upw 리터럴(getenv 주석) | getenv 복구 |
| `40…/41.tradingKIS_v3.0.py:498-500` | 보안 | `app_key`/`app_secret`/`account_no` 리터럴 | 환경변수 |
| `20…/21…:52`, `50…:2194`, ETF DB URL들 | 보안 | MySQL 비밀번호 리터럴 | 환경변수 |
| 루트 `.gitignore` (전체) | 저장소분리 | `naverPub/` 미무시; 루트가 naverPub 파일 30개 추적 | `naverPub/`를 루트 ignore + 루트 인덱스에서 untrack |
| 중첩 `tradingKRX/.git` + `naverPub/.git` | 저장소분리 | 동일 논리 파일이 양쪽에 dirty (점검 시점) | 커밋은 저장소별로만; 루트에서 naverPub 추적 중단 |
| `.cursor/rules/krx-data-fetch.mdc:29-37` | 문서 | OTP/Referer **https** 정합 완료 | **해결** |
| `naverPub/krx_client.py:30-36` | 준수 | `BASE=https://data.krx.co.kr`, Referer·로그인 | 유지·타 폴더 표준으로 승격 |
| `naverPub/notify.py:199-201` | 죽은코드(약) | `notify_bundle` deprecated, runner 오류경로만 사용 | 유지 또는 정리 |
| `naverPub/render.py:945-946` | 잔재 | `capture/`, `*_sec*.png` 정리 로직 | 레거시 정리 유지 OK |
| `30…/32.ETF_PDF_v1.0.py` | 구버전 | → `archive/` 이관 | **해결** |
| `content_market` vs `21` Talent | 정합위반 | 동명·이질 정의 | 정본 하나로 통일 후 이식 |

**데이터 원천**

- `.go.kr` 사용: **없음** (준수).
- naverPub·`00.v3`·`10`·`32.v2` OTP/JSON: **https + Referer** 확인.
- `31.ETF_ohlcv_v6.0`: KRX OTP/Referer 경로 없음(pykrx).

**예외·로깅**

- naverPub: `logging` + runner `errors` 집약 — 양호.
- 10~50 Spyder 스크립트: `print` 중심 — 환경 차이로 허용되나 이식 시 로깅 통일 필요.
- N+1/성능: `content_etf`·스크리닝 종목별 로드 패턴 존재 — **정량 프로파일은 미확인**.

---

## 5. 개선 로드맵

### 리스크5 (구버전·http/https) — **해결** (2026-07-26)

- 구버전: `archive/` 이관 + git 추적 유지 (`archive/README.md`)
- 활성 10~50: 폴더당 최신 1개만 (병존 0)
- `data.krx.co.kr` 문서·주석·규칙 **https 전용** (http 잔여 0건)
- `__pycache__/` · `*.py[cod]` 는 `.gitignore` 포함

### P0 — 정합성 (지표 단일 정본)

| 액션 | 재활용 가능한 기존 구현 |
|------|------------------------|
| 에너지: raw vs tanh(K=15) 중 정본 확정. 규칙상 K=15면 20/40/50에 동일식 이식 또는 규칙 문구를 “발행만”으로 한정 | `naverPub/content_market.py:26-27,372-403` |
| Talent: 시가→종가 일수(`21:1136-1149`) vs 종가등락 가중지수(`content_market:696,787`) 중 하나 선택·문서화 | 일수 정본=`21`/`collect_daily`; 지수는 별도 이름 권장 |
| RS 표시 평균 구간 통일 (10~120 vs 20~200) | 소스 periods=`collect_daily`/`11`; 표시 평균은 한쪽으로 고정 |
| ATR: Wilder(talib) vs SMA(TR) 통일 | 스크리닝=`50`/`screening`; 변동성 산점도=`content_volatility:204-216` — 선택 후 이식 |
| CVI TV 소스(`close*volume` vs `trading_value`) 문서화·가능하면 통일 | `21:4355+`, `content_volatility:456-490` |

### P1 — 보안

| 액션 | 재활용 |
|------|--------|
| 로컬 스크립트 시크릿 전부 getenv / `.env` (커밋 금지) | `naverPub/config.py` 패턴 |
| git history에 시크릿 잔존 여부 점검 | **미확인** — 별도 히스토리 스캔 필요 |
| KIS 키 로테이션(이미 리포에 노출) | `40…:498-500` |

### P2 — 저장소 분리·중복 제거

| 액션 | 재활용 |
|------|--------|
| 루트 `.gitignore`에 `naverPub/` 추가 후 `git rm -r --cached naverPub` (중첩 repo 유지) | `project-structure.mdc:33-38` |
| 공통 `krx_client`를 로컬 10/30/`00`이 import 가능하게 패키지화(또는 복사 동기 스크립트) | `naverPub/krx_client.py` |
| 스크리닝·RS·breadth 공유 모듈화 | `screening.py` ↔ `51`; `collect_daily` ↔ `11` |

### P3 — 성능·품질

| 액션 | 비고 |
|------|------|
| ETF PDF·스크리닝의 종목별 반복 쿼리 배치화 | 프로파일 후 진행(현재 정량 **미확인**) |
| `notify_bundle`/레거시 capture 정리 | 영향 범위 작음 |
| `krx-data-fetch.mdc` http→https 문서 정정 | **해결** — OTP/Referer 모두 `https://data.krx.co.kr` |

---

## 6. 근거 (파일:라인)

### 6.1 규칙 정본

- 폴더 역할·지표 공통·저장소 분리·pykrx 금지·https: `.cursor/rules/project-structure.mdc:11-53`
- OTP/Referer https: `.cursor/rules/krx-data-fetch.mdc` (http 잔여 **해결**)
- 구버전 archive 방침: `archive/README.md`

### 6.2 지표

```26:27:naverPub/content_market.py
# 방향(상승/하락) tanh 가중: energy × (1 + tanh(수익률%/K))
ENERGY_DIR_K = 15.0
```

```1136:1149:20. MARKET Analysis/21. KRX_market_analysis_v2.0.py
def _talent_days_from_ohlcv(g: pd.DataFrame, window: int, thr: float = 0.10) -> float:
    """최근 window 거래일 중 (종가 ≥ 시가×(1+thr))인 날 수(일)."""
    ...
    r = (cl[m].astype(float) / op[m].astype(float)) - 1.0
    return float((r >= thr).sum())
```

```696:696:naverPub/content_market.py
    talent 지수 = (n20/20)*0.5 + (n50/50)*0.3 + (n120/120)*0.2
```

```204:216:naverPub/content_volatility.py
def _add_atr_over_close(df: pd.DataFrame) -> pd.DataFrame:
    ...
    out["atr14"] = tr.groupby(out["ticker"], sort=False).transform(
        lambda s: s.rolling(ATR_N, min_periods=ATR_N).mean()
    )
```

```38:50:naverPub/content_picking.py
WEIGHT_SETS: dict[str, dict[str, float]] = {
    "long": {
        "RS": 0.50,
        "주가위치": 0.30,
        ...
```

```1805:1807:40. Trading/41. tradingKIS_v3.0.py
                er3_s = f"{float(np.mean(ers[:3])):.3f}"
                ...
                er3_s = f"{float(np.mean(ers)):.3f}"
```

```30:36:naverPub/krx_client.py
BASE = "https://data.krx.co.kr"
OTP_URL = f"{BASE}/comm/fileDn/GenerateOTP/generate.cmd"
...
```

### 6.3 pykrx

- `10. OHLCV/11. KRX_ohlcv_v4.0.py:31` — `from pykrx import stock`
- `30. ETF/31. ETF_ohlcv_v6.0.py:40` — `from pykrx import stock`
- `00. krx_info_v2.0.py:19` — `from pykrx import stock`

### 6.4 저장소

- 루트 `.gitignore`: `naverPub/` 항목 **없음** (파일 10줄, `.env`만)
- `git ls-files naverPub/*` (루트): **30 files tracked**
- 중첩: `tradingKRX/.git`, `naverPub/.git` 둘 다 존재
- naverPub remote: `https://github.com/kaysh0823/naverpub.git`
- 루트 remote: **없음**(점검 시점 `git remote -v` 공백)
- 점검 시점 양쪽에 동일 계열 수정 dirty (`collect_daily`, `content_*` 등)

### 6.5 시크릿 (값 마스킹)

- `00.krx_info_v3.0.py:28` — `KRX_PW='***'`
- `10…/11…:37`, `passwd='***'` 다수 라인
- `40…/41…:499` — `app_secret = "***"`
- `50…/51…:2194` — `passwd='***'`
- naverPub: `config.py` getenv — **하드코딩 할당 없음**(양호). `.env`는 gitignore

---

## 부록: 미확인 항목

- VPS 실환경 `.env`·cron 동작 상태
- git history에 과거 시크릿 잔존 여부
- talib ATR vs SMA(TR)의 수치 괴리 규모(샘플 백테스트 미실시)
- Spyder 로컬 미동기 복사본
- 루트 저장소의 의도된 원격 URL(현재 remote 미설정)

---

*본 리포트는 코드 변경 없이 작성됨. 경로는 워크스페이스 기준 상대 경로.*
