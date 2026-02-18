---
title: "【PostgreSQL v17】ウィンドウ関数と集計を極める：RANK, ROW_NUMBER, GROUPING 完全ガイド"
emoji: "🐘"
type: "tech" # tech: 技術記事 / idea: アイデア
topics: ["postgresql", "sql", "docker", "db", "learning"]
published: true
---

データベースの授業で `GROUP BY` を習ったとき、「便利だけど、集計前の元の行の情報が消えてしまうのが不便だな」と思ったことはありませんか？あるいは、「部門ごとの売上トップ3を出したい」という要件に対し、複雑な自己結合やサブクエリを書いて苦戦した経験はないでしょうか。

この記事では、**PostgreSQL 17** のコンテナ環境を用いて、分析クエリの要となる**ウィンドウ関数 (Window Functions)** と、レポート作成に必須の**高度な集計 (GROUPING)** について解説します。

想定読者は「DBの授業を一通り履修し、さらに実践的なSQL力をつけたい方」です。
単に読むだけでなく、実際にコンテナを立ち上げ、SQLを実行し、演習問題に取り組むことで、**最低でも90分間** みっちりと脳に汗をかける構成になっています。

:::message
**前提知識**
基本的な `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY` の書き方を理解していること。
:::

## 0. 実験環境の準備

本記事のハンズオンは、講義で指定されている PostgreSQL 17 (Dockerコンテナ) 環境で行うことを想定しています。以下の講義資料（第2回）の手順に基づき、演習環境を起動してデータベースに接続してください。

🔗 [第2回講義資料: 演習環境の構築](https://takeshiwada1980.github.io/DB-2025/lecture02.html)

（※ 講義資料等の引用・リンクについては担当教員の許可を得て記載しています）

### サンプルデータの作成

DBへの接続が完了したら、以下のSQLを実行して実験用のサンプルデータを作成しましょう。ここでは、架空の「ゲームのスコアデータ」と「店舗の売上データ」を作成します。
**順位付けの挙動（同点の場合どうなるか）** を確認するため、わざと重複データを含めています。

```sql
-- テーブル作成
CREATE TABLE game_scores (
    player_name VARCHAR(20),
    team        VARCHAR(10),
    score       INT
);

CREATE TABLE sales_data (
    region      VARCHAR(10),
    shop_name   VARCHAR(20),
    amount      INT
);

-- データ投入 (わざと同点やNULLを含めています)
INSERT INTO game_scores (player_name, team, score) VALUES
('Alice',   'Red',  100),
('Bob',     'Red',  85),
('Charlie', 'Red',  100), -- Aliceと同点
('Dave',    'Blue', 120),
('Eve',     'Blue', 95),
('Frank',   'Blue', 120), -- Daveと同点
('Grace',   'Blue', 80);

INSERT INTO sales_data (region, shop_name, amount) VALUES
('Kanto',  'Tokyo',    5000),
('Kanto',  'Yokohama', 3000),
('Kanto',  'Chiba',    NULL), -- データ欠損
('Kansai', 'Osaka',    4500),
('Kansai', 'Kyoto',    4200),
('Kansai', 'Kobe',     4500); -- Osakaと同額
```

データ投入後に内容を確認して、以降の実験で参照するレコードをイメージしておきましょう。

```sql
SELECT * FROM game_scores ORDER BY team, score DESC;
```

| player_name | team | score |
| :--- | :--- | ---: |
| Dave | Blue | 120 |
| Frank | Blue | 120 |
| Eve | Blue | 95 |
| Grace | Blue | 80 |
| Alice | Red | 100 |
| Charlie | Red | 100 |
| Bob | Red | 85 |

```sql
SELECT * FROM sales_data ORDER BY region, amount DESC NULLS LAST;
```

| region | shop_name | amount |
| :--- | :--- | ---: |
| Kansai | Osaka | 4500 |
| Kansai | Kobe | 4500 |
| Kansai | Kyoto | 4200 |
| Kanto | Tokyo | 5000 |
| Kanto | Yokohama | 3000 |
| Kanto | Chiba | NULL |

この2つのテーブルを頭に入れたうえで、次章からウィンドウ関数の挙動を観察していきます。

---

## 1. ウィンドウ関数の基本と PARTITION BY

### ウィンドウ関数とは？
通常の `GROUP BY` は行を「畳み込み（集約）」ます。対してウィンドウ関数は、**「行はそのまま維持しつつ、別の行の集まり（ウィンドウ）を集計して、現在の行に付加する」** 機能です。ここでいう「別の行の集まり（ウィンドウ）」とは、`PARTITION BY` や `ORDER BY` で定義した、**「現在の行と一緒に計算に含める仲間の行セット」** を指します。例えば `PARTITION BY team ORDER BY score DESC` と書けば、「同じ `team` に属する行を、`score` の降順に並べたグループ」がそのままウィンドウになります。

ウィンドウはおおまかに次の3つの要素で構成されます。

1. **PARTITION BY**: どのグループごとに計算するか（例: team 単位）
2. **ORDER BY**: グループ内での並び順（例: score 降順）
3. **フレーム句**: ORDER BY された行のうち、どこからどこまで見るか（今回はデフォルト＝全行を利用）

どの要素が欠けても、求めたい統計が想定どおりの行に紐づかないので、シナリオに応じて組み立てます。

### ウォームアップ: シンプルな `OVER()` を動かしてみる

まずは `PARTITION BY` を付けず、テーブル全体を1つのウィンドウとして扱う例で挙動を体感しましょう。

```sql
SELECT
    player_name,
    score,
    COUNT(*) OVER() AS total_rows,
    AVG(score) OVER()   AS avg_score
FROM game_scores
ORDER BY score DESC;
```

**実行結果（抜粋）:**

| player_name | score | total_rows | avg_score |
| :--- | ---: | ---: | ---: |
| Dave | 120 | 7 | 100.0 |
| Frank | 120 | 7 | 100.0 |
| Alice | 100 | 7 | 100.0 |
| Charlie | 100 | 7 | 100.0 |
| Bob | 85 | 7 | 100.0 |

`COUNT(*) OVER()` や `AVG(score) OVER()` と書くと、**行を削らずに** 全行の件数・平均が各レコードに付与されることが分かります。以降の実験ではここに `PARTITION BY` を加えて、ウィンドウの範囲を自在に切り替えていきます。

### 実験1: 全体での集計 vs PARTITION BY

まずは、チームごとの最高スコアを、各プレイヤーの行の横に表示してみましょう。

```sql
SELECT 
    player_name,
    team,
    score,
    -- (A) 全体の中での最大値
    MAX(score) OVER() as max_overall,
    -- (B) チーム内での最大値 (PARTITION BY)
    MAX(score) OVER(PARTITION BY team) as max_team
FROM game_scores;
```

**実行結果:**

| player_name | team | score | max_overall | max_team |
| :--- | :--- | :--- | :--- | :--- |
| Dave | Blue | 120 | 120 | 120 |
| Frank | Blue | 120 | 120 | 120 |
| Eve | Blue | 95 | 120 | 120 |
| Grace | Blue | 80 | 120 | 120 |
| Alice | Red | 100 | 120 | 100 |
| Charlie | Red | 100 | 120 | 100 |
| Bob | Red | 85 | 120 | 100 |

**解説:**
* `(A) OVER()`: 条件なし。テーブル全体の最大値（120）が全行に付与されます。
* `(B) PARTITION BY team`: 「チーム」という窓（ウィンドウ）の中で最大値を計算します。Redチームの行にはRedチーム内の最大値（100）がつきます。
* **重要:** 行数は減っていません。これが `GROUP BY` との決定的な違いです。

同じ `MAX()` でも、`OVER()` でウィンドウ化することで「行を失わずに集約を列として持てる」点が肝です。`PARTITION BY` を少し変えるだけで、「チーム全体」「リーグ全体」「プレイヤーの直近3試合」といった粒度を自在に切り替えられます。以降のセクションではこの柔軟性を活かして、ランキングや集計レポートを効率化していきます。

この土台が理解できれば、次に解説する順位付け関数の違いも直感的に捉えられるようになります。

---

## 2. 順位付け関数の違い (RANK vs ROW_NUMBER)

「ランキング」を作るとき、同点の扱いによって関数を使い分ける必要があります。ここがテストや実務で最も問われるポイントです。

### 実験2: 3つの順位付け関数を比較する

以下のクエリを実行し、`Red` チームの `Alice` と `Charlie` (共に100点) の順位に注目してください。

```sql
SELECT 
    player_name,
    team,
    score,
    -- (1) 単純な連番
    ROW_NUMBER() OVER(PARTITION BY team ORDER BY score DESC) as row_num,
    -- (2) 同順位は同じ値、次は飛ばす
    RANK()       OVER(PARTITION BY team ORDER BY score DESC) as rnk,
    -- (3) 同順位は同じ値、次は飛ばさない
    DENSE_RANK() OVER(PARTITION BY team ORDER BY score DESC) as dense_rnk
FROM game_scores;
```

**実行結果 (Redチーム抜粋):**

| player_name | team | score | row_num | rnk | dense_rnk |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Alice | Red | 100 | **1** | **1** | **1** |
| Charlie | Red | 100 | **2** | **1** | **1** |
| Bob | Red | 85 | **3** | **3** | **2** |

**解説:**
1.  **ROW_NUMBER**: 同点だろうと関係なく、必ず `1, 2, 3...` と一意な番号を振ります（順序は不定、または他の列で決まる）。「とにかく上位3件を取得したい」などのページング処理で使います。
2.  **RANK**: 同点は「1位」。その分、次の順位が飛びます（2人が1位なら、次は3位）。オリンピックなどの競技形式です。
3.  **DENSE_RANK**: 同点は「1位」。次は飛ばさずに「2位」になります。順位の数値に穴を開けたくない場合に使います。

> 🔧 小技: PostgreSQLなら `SELECT DISTINCT ON (team) ... ORDER BY team, score DESC` で「各チーム1位だけ取得」もできますが、上位N件や同点処理を柔軟にしたい場合はウィンドウ関数のほうが圧倒的にスケーラブルです。

次は「集計レベルを自動で切り替える」`ROLLUP` と `GROUPING` を扱います。ランキングで得た知見が、今度はレポート作成でどう活きるのかを見ていきましょう。

---

## 3. GROUPING と ROLLUP (高度な集計)

ここではウィンドウ関数から少し離れ、`GROUP BY` の拡張機能について触れます。
レポート作成で「小計（Subtotal）」や「総計（Grand Total）」を出したい場合、`UNION ALL` で繋げたりしていませんか？ `ROLLUP` を使えば一発です。

しかし、`ROLLUP` を使うと「集計行」のラベルが `NULL` になります。**「元のデータのNULL」なのか「集計結果のNULL（総計行）」なのか**を見分けるために使うのが `GROUPING` 関数です。

### 実験3: 売上レポートの作成

`sales_data` を使い、地域ごとの小計と、全体の総計を出します。

```sql
SELECT 
    region,
    shop_name,
    SUM(amount) as total_sales,
    -- 集計行かどうかを判定 (1なら集計行、0なら通常のデータ)
    GROUPING(region) as grp_region,
    GROUPING(shop_name) as grp_shop
FROM sales_data
GROUP BY ROLLUP(region, shop_name)
ORDER BY region NULLS LAST, shop_name NULLS LAST;
```

**実行結果 (一部抜粋・整形):**

| region | shop_name | total_sales | grp_region | grp_shop | 解説 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Kansai | Kobe | 4500 | 0 | 0 | 通常行 |
| Kansai | Kyoto | 4200 | 0 | 0 | 通常行 |
| Kansai | Osaka | 4500 | 0 | 0 | 通常行 |
| **Kansai** | **NULL** | **13200** | **0** | **1** | **関西エリア小計** |
| ... | ... | ... | ... | ... | ... |
| **NULL** | **NULL** | **21200** | **1** | **1** | **全社総計** |

**解説:**
* `ROLLUP(A, B)` は、`(A, B)`, `(A)`, `()` の3パターンの集計を一度に行います。
* `GROUPING(カラム名)` は、その行が「そのカラムを集約して作られた行（小計行）」であれば `1` を返します。
* これを使えば、`CASE WHEN GROUPING(shop_name) = 1 THEN '小計' ELSE shop_name END` のようにして綺麗なレポートが作れます。

> 🔧 小技: `ROLLUP` に慣れたら、`GROUPING SETS` で「関西だけ小計」「全国で月次合計」など任意の組み合わせも作れます。`GROUPING` 関数はそのまま再利用できるので、複雑な帳票でもラベル付けロジックは共通化可能です。

---

## 4. 【実践演習】90分チャレンジ

ここからは手を動かすフェーズです。これまで確認した「ウィンドウで行を保持する」「ランキングで同点制御する」「GROUPINGで集計行を見分ける」という3本柱を、自力で組み合わせてみましょう。

ここからが本番です。以下の3つの課題に取り組んでください。
実際にSQLを書き、想定通りの結果が出るまで試行錯誤することが学習の肝です。

### 演習問題 1: 部門別TOP N抽出 (難易度: ★★☆)

`game_scores` テーブルを使用してください。
各チーム (`team`) ごとに、スコアが高い順に上位2名のプレイヤーを抽出してください。
ただし、**同点の場合は両方抽出**してください（例：1位が2人いたら、2人とも出力し、次は3位扱い）。

> 💡ヒント: `PARTITION BY team ORDER BY score DESC` で `RANK()` を付与すると、同点を同順位のまま保持できます。メインクエリの `WHERE` ではウィンドウ関数が使えないため、CTEやサブクエリに一度まとめてから順位でフィルタしましょう。

**出力イメージ:**
```
 team | player_name | score | rank_in_team 
------+-------------+-------+--------------
 Blue | Dave        |   120 |            1
 Blue | Frank       |   120 |            1
 Red  | Alice       |   100 |            1
 Red  | Charlie     |   100 |            1
```
※ Redチームは2位が存在しない（1位が2名のため次は3位）ので、100点の2名だけでOKです。もし3位（85点）まで含めたい場合は条件を考えてみてください。

---

### 演習問題 2: 前後の行との比較 (難易度: ★★☆)

`sales_data` テーブルを使用してください。
`Kansai` 地域の店舗について、売上金額順（降順）に並べ、**「自分より一つ順位が上の店舗との売上差」** を計算してください。1位の店舗は差分なし（NULL）で構いません。

※ ヒント: ウィンドウ関数 `LAG` を使うか、これまでに習った関数を工夫します。今回は `LAG` を調べて使ってみましょう。

> 💡ヒント: `ORDER BY amount DESC` で並べた状態に `LAG(amount)` を重ねると、「直前の行＝一つ上の順位」の金額を取得できます。`LAG` の結果と現在の `amount` を使って差分を計算してみてください。

**出力イメージ:**
```
 shop_name | amount | diff_to_prev 
-----------+--------+--------------
 Kobe      |   4500 |         NULL
 Osaka     |   4500 |            0  (Kobeとの差)
 Kyoto     |   4200 |          300  (Osakaとの差)
```
*(注意: ORDER BYの順序によってKobeとOsakaの順序は入れ替わる可能性があります)*

---

### 演習問題 3: 完璧なレポート作成 (難易度: ★★★)

`sales_data` テーブルを使用してください。
`GROUPING` と `CASE` 式を駆使して、以下のような「NULLを含まない、人間が読みやすい集計表」を作成してください。

**条件:**
1.  `region` が集計行の場合は「全地域計」と表示。
2.  `shop_name` が集計行の場合は「地域計」と表示。
3.  `amount` が `NULL` のデータ（千葉）は、集計結果としては `0` として扱い、表示も `0` にする（`COALESCE` を使用）。

> 💡ヒント: `GROUP BY ROLLUP(region, shop_name)` で地域別・ショップ別・総計までまとめて算出できます。`GROUPING(region)` / `GROUPING(shop_name)` の戻り値を `CASE` 式で判定し、ラベル文字列を差し替えるとNULLを避けられます。`COALESCE(amount, 0)` を忘れずに。

**目標出力:**
```
   地域    |   店舗   | 売上合計 
-----------+----------+----------
 Kansai    | Kobe     |     4500
 Kansai    | Kyoto    |     4200
 Kansai    | Osaka    |     4500
 Kansai    | 地域計   |    13200
 Kanto     | Chiba    |        0
 Kanto     | Tokyo    |     5000
 Kanto     | Yokohama |     3000
 Kanto     | 地域計   |     8000
 全地域計  |          |    21200
```

---

## 5. 解答例

まずは自分で考えてから、以下の解答を確認してください。

<details>
<summary><strong>演習問題 1 の解答</strong> (クリックして展開)</summary>

「同点の場合は両方抽出」という条件から、`ROW_NUMBER` ではなく `RANK` を使用します。その後、サブクエリ（またはCTE）で `rank <= 2` でフィルタリングします。

```sql
WITH RankedScores AS (
    SELECT 
        team,
        player_name,
        score,
        RANK() OVER(PARTITION BY team ORDER BY score DESC) as rank_in_team
    FROM game_scores
)
SELECT * FROM RankedScores
WHERE rank_in_team <= 2;
```
※ `WHERE` 句で直接ウィンドウ関数は使えないため、サブクエリにする必要があります。ここがよくあるハマりポイントです。

このアプローチなら、ウィンドウ関数で各行の順位を保持したまま条件抽出できるため、自己結合で順位テーブルを再構築するより読みやすく、上位N件の抽出条件も簡潔に表現できます。

</details>

<details>
<summary><strong>演習問題 2 の解答</strong> (クリックして展開)</summary>

`LAG(カラム名) OVER(...)` は、ウィンドウ内の「1つ前の行の値」を取得する関数です。

```sql
SELECT 
    shop_name,
    amount,
    -- 現在の行のamount - 1つ前の行のamount
    LAG(amount) OVER(ORDER BY amount DESC) - amount as diff_to_prev_wrong_sign,
    -- 正しくは「前の行(高い) - 今の行(低い)」なら
    LAG(amount) OVER(ORDER BY amount DESC) - amount as diff_val
FROM sales_data
WHERE region = 'Kansai';
```
※ より厳密に `PARTITION BY region` を入れても良いですが、WHERE句で絞っているので今回は不要です。

`LAG` を使って前行の値を直接参照できるので、自己結合やサブクエリで「一つ上の順位」を探す必要がなくなり、差分計算のロジックを1行で表現できます。ランキング系の分析をシンプルに保てるのが大きな利点です。

</details>

<details>
<summary><strong>演習問題 3 の解答</strong> (クリックして展開)</summary>

`GROUPING` 関数の戻り値（0または1）を利用して、表示する文字列を制御します。

```sql
SELECT 
    CASE 
        WHEN GROUPING(region) = 1 THEN '全地域計'
        ELSE region 
    END as 地域,
    CASE 
        WHEN GROUPING(shop_name) = 1 THEN '地域計'
        ELSE shop_name 
    END as 店舗,
    SUM(COALESCE(amount, 0)) as 売上合計
FROM sales_data
GROUP BY ROLLUP(region, shop_name)
ORDER BY region NULLS LAST, shop_name NULLS LAST;
```
※ `COALESCE(amount, 0)` は、NULLを0に変換する関数です。これを `SUM` の中に入れることで、NULLのデータも正しく0として集計されます。

`ROLLUP` と `GROUPING` を組み合わせることで、総計・小計を一度に導出しつつラベル付けもSQL側で完結できます。複数の UNION を組むよりも処理コストが低く、アプリケーション側の後処理も最小化できます。

</details>

---

## おわりに

ウィンドウ関数とGROUPINGを使いこなせると、アプリケーション側（PythonやPHPなど）でループ処理を書いて計算していたロジックの多くを、DB側で高速に処理できるようになります。

今回の演習で作成したSQLは、実際のデータ分析基盤や業務システムのレポート出力でも頻出のパターンです。ぜひ、ご自身のプロジェクトでも活用してみてください。