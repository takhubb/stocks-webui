# stocks-webui

J-Quants API を使って、日本株の財務指標と株価推移を WebUI で可視化するサンプルです。4桁コードを入力して実行すると、以下を 1 画面で確認できます。

- `PSR / PER / PBR` の折れ線グラフ
- 売上高、営業利益、経常利益、純利益の棒グラフ
- 売上高、営業利益、経常利益、純利益の前年同期比の折れ線グラフ
- 株価終値の週足グラフと `25週 / 50週` 移動平均線
- 時価総額の週足グラフ
- 出来高の折れ線グラフ
- 自己資本比率、`ROE`、`ROA`、`PEG`
- 業種、業種平均 `PSR / PER`

## 前提

- J-Quants の `API Key` が必要です
- `.env` に `JQUANTS_API_KEY` を設定してください
- 業種平均 `PSR / PER` は bulk CSV をローカルキャッシュして計算します

## セットアップ

`.env` を作成します。

```bash
cp .env.example .env
```

`.env` の例:

```env
JQUANTS_API_KEY=your_api_key_here
JQUANTS_BULK_MONTHS=18
```

## Docker で起動

```bash
docker compose up --build
```

起動後、以下を開きます。

```text
http://localhost:8000
```

初回実行時は業種平均計算用の bulk データを `./cache` に保存するため、少し時間がかかります。

## ローカル実行

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## 指標の考え方

- `PSR / PER / PBR` は開示日時点の株価と、その時点で算出できる財務値から概算しています
- `ROE / ROA` は直近 `TTM` ベースです
- `PEG` は直近の予想 `EPS` と前期実績 `EPS` の成長率から概算しています
- `業種平均 PSR / PER` は J-Quants の `S33` 業種単位で集計しています
- IFRS 企業では `経常利益 (OdP)` が空欄のことがあるため、その系列は欠損する場合があります

## 使用 API

- `GET /v2/equities/master`
- `GET /v2/equities/bars/daily`
- `GET /v2/fins/summary`
- `GET /v2/bulk/list`
- `GET /v2/bulk/get`

## ディレクトリ

```text
app/
  main.py
  services/
  static/
  templates/
cache/
Dockerfile
docker-compose.yml
requirements.txt
```
