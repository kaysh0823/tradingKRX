# naverPub VPS 배포 가이드

국내 리전 Ubuntu VPS에서 **매 영업일 16:00 KST**에 실행하는 네이버 프리미엄콘텐츠용 데일리 생성기입니다.  
로컬 PC MySQL에 의존하지 않고, VPS 자체 MySQL에 수집·적재합니다.

## 1. 시스템 준비

```bash
sudo timedatectl set-timezone Asia/Seoul
timedatectl   # Time zone: Asia/Seoul 확인

sudo apt update
sudo apt install -y python3.11 python3.11-venv python3-pip \
  mysql-server git fonts-nanum fonts-nanum-coding
fc-list | grep -i nanum   # NanumGothic 확인
```

## 2. MySQL

```bash
sudo mysql -e "
CREATE DATABASE IF NOT EXISTS naverpub CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'naverpub'@'localhost' IDENTIFIED BY 'STRONG_PASSWORD';
GRANT ALL PRIVILEGES ON naverpub.* TO 'naverpub'@'localhost';
FLUSH PRIVILEGES;
"
```

## 3. 앱 설치

```bash
sudo mkdir -p /opt/naverPub
sudo rsync -a ./ /opt/naverPub/   # 또는 git clone
sudo chown -R $USER:$USER /opt/naverPub
cd /opt/naverPub

python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
# Linux에 시스템 의존성 부족 시:
# playwright install-deps chromium

cp .env.example .env
chmod 600 .env
nano .env   # DB_*, KRX_ID, KRX_PW, TELEGRAM_* 입력
```

`.env` 예시는 저장소의 `.env.example` 참고. **시크릿을 코드에 넣지 마세요.**

스키마 생성(최초 1회, runner가 자동 생성도 함):

```bash
cd /opt/naverPub && source .venv/bin/activate
python -c "from db import ensure_schema; ensure_schema(); print('ok')"
```

## 4. 최초 이력 이관 (최소 400거래일)

RS·신고가(250일)·talent(120일)에 **최소 약 400거래일** OHLCV가 필요합니다.

### 방법 A — mysqldump (권장, 로컬 PC에서)

로컬 `kor_stock_db`:

```bash
# 최근 2년치 ohlcv (날짜는 환경에 맞게 조정)
mysqldump -uroot -p kor_stock_db krx_ohlcv \
  --where="date >= DATE_SUB(CURDATE(), INTERVAL 750 DAY)" \
  --no-create-info --complete-insert > ohlcv_dump.sql

mysqldump -uroot -p kor_stock_db krx_index_ohlcv \
  --where="ticker IN ('1001','2001') AND date >= DATE_SUB(CURDATE(), INTERVAL 750 DAY)" \
  --no-create-info --complete-insert > index_dump.sql

mysqldump -uroot -p kor_stock_db krx_etf_pdf \
  --where="수집일자 >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)" \
  --no-create-info --complete-insert > pdf_dump.sql
```

VPS에서 스키마가 `ohlcv` / `index_ohlcv` / `etf_pdf` 이므로, dump의 테이블명을 바꾸거나  
아래 **방법 B(CSV)** 를 쓰세요. dump를 직접 넣을 경우:

```bash
# 예시: 임시 테이블로 적재 후 INSERT…SELECT 로 매핑
# (컬럼명이 다를 수 있어 CSV 경로를 권장)
```

### 방법 B — CSV (migrate_initial.py)

**로컬 PC** (`.env`에 `LOCAL_DB_*` 설정):

```bash
cd naverPub
python migrate_initial.py export-all --dir migrate_data --min-days 400
# → migrate_data/ohlcv.csv, index_ohlcv.csv, etf_pdf.csv
```

파일을 VPS로 복사 후:

```bash
scp migrate_data/*.csv user@vps:/opt/naverPub/migrate_data/
ssh user@vps
cd /opt/naverPub && source .venv/bin/activate
python migrate_initial.py import-ohlcv --file migrate_data/ohlcv.csv
python migrate_initial.py import-index --file migrate_data/index_ohlcv.csv
python migrate_initial.py import-pdf --file migrate_data/etf_pdf.csv
```

로컬 `krx_ohlcv`에 `market`/`name`/`mcap`이 없으면 이관 후  
첫 영업일 수집(`runner.py`)이 `tickers`·당일 `mcap`을 채웁니다.  
에너지배율은 **시총(mcap)** 이 필요하므로, 가능하면 로컬에서 시총 컬럼을 붙여 export하세요.

이관 후 RS·talent 백필(최신 1일):

```bash
python -c "
from datetime import date
from collect_daily import compute_rs_for_date, compute_talent_for_date
from db import engine
import pandas as pd
d = pd.read_sql('SELECT MAX(date) AS d FROM ohlcv', engine()).iloc[0]['d']
d = pd.to_datetime(d).date()
print('rs', compute_rs_for_date(d))
print('talent', compute_talent_for_date(d))
"
```

## 5. cron (권장)

```bash
chmod +x /opt/naverPub/deploy/run_daily.sh
crontab -e
```

`deploy/crontab.example` 내용:

```cron
CRON_TZ=Asia/Seoul
0 16 * * 1-5 /opt/naverPub/deploy/run_daily.sh >> /opt/naverPub/logs/cron.log 2>&1
```

수동 테스트:

```bash
/opt/naverPub/deploy/run_daily.sh
# 또는
cd /opt/naverPub && source .venv/bin/activate
python runner.py --skip-notify
python runner.py --force --date YYYYMMDD   # 휴장/백필 테스트
```

## 6. systemd timer (대안)

```bash
sudo cp deploy/naverpub.service deploy/naverpub.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now naverpub.timer
systemctl list-timers | grep naverpub
```

## 7. 산출물

`outputs/YYYYMMDD/`

| 파일 | 설명 |
|------|------|
| `daily_snapshot_YYYYMMDD/` | 데일리 스냅샷 (에너지·신고가·신저가·RS) png/xlsx/csv |
| `active_etf_pdf_YYYYMMDD/` | 액티브 ETF PDF 구성 png/xlsx/csv |

단독 실행: `python runner.py --force --daily-only` / `--etf-only`

텔레그램: `.env`에 `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`가 있으면 이미지+본문 전송. 없으면 스킵.

## 8. 수집 규칙 요약

- **pykrx 금지** — KRX `data.krx.co.kr` OTP CSV / getJsonData만 사용
- 전종목 시세: `[12001]` `MDCSTAT01501` `mktId=ALL` **하루 1 CSV**
- ETF PDF: 종목명에 `액티브` 포함 ETF만
- 영업일: Timeout/ConnectionError → 동일 일자 재시도 후 `RuntimeError`  
  정상+빈 데이터만 휴장 처리
- 휴장일(오늘 ≠ 최신 영업일): 로그만 남기고 exit 0

## 9. 디렉터리

```
naverPub/
  runner.py
  krx_client.py
  collect_daily.py
  content_market.py
  content_etf.py
  render.py
  notify.py
  migrate_initial.py
  config.py
  db.py
  .env.example
  requirements.txt
  deploy/
  outputs/
```
