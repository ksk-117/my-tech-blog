---
title: "【PostgreSQL v17】ボイスコッド正規化と第4・第5正規化を体で覚えるハンズオン"
emoji: "🐘"
type: "tech"
topics: ["postgresql", "db", "normalization", "data-modeling", "learning"]
published: false
---

データベース設計の授業で第3正規形までは触れたけれど、**「BCNF 以降はテストで名前を覚えただけ」** という方は多いのではないでしょうか？しかし現場では、複雑な分析基盤やマスタデータ統合を行う際に、**ボイスコッド正規形 (BCNF)、第4正規形 (4NF)、第5正規形 (5NF)** を理解しているかどうかが、テーブル設計の品質に直結します。

この記事では PostgreSQL 17 のコンテナ環境を使い、実際にテーブルを作って分解しながら高位正規形の要点をつかみます。90分程度で、BCNF/4NF/5NF の違いと実務での使いどころを押さえられる構成です。

:::message
**前提知識**
- 関数従属性 (Functional Dependency) の基本
- 第3正規形までの分解手順
- `psql` や `docker` を使った PostgreSQL 操作
:::

## 0. 実験環境の準備

前回の記事と同じ PostgreSQL 17 (Docker) 環境を利用します。未構築の場合は、講義資料の手順に沿って起動してください。

🔗 [第2回講義資料: 演習環境の構築](https://takeshiwada1980.github.io/DB-2025/lecture02.html)

（※ 講義資料・リンクは担当教員の許可を得て掲載しています）

### サンプルデータの投入

以下の 3 テーブルを作成し、BCNF/4NF/5NF の分解を順に試します。

```sql
-- イベントと担当者 (部分依存あり)
CREATE TABLE event_assignments (
    event_id    INT,
    hall_id     INT,
    organizer   TEXT,
    contact     TEXT
);

-- 複数値依存を含むテーブル
CREATE TABLE artist_skills (
    artist_id   INT,
    skill       TEXT,
    instrument  TEXT
);

-- 結合従属性を含むテーブル
CREATE TABLE supplier_parts_projects (
    supplier    TEXT,
    part        TEXT,
    project     TEXT,
    notes       TEXT
);

INSERT INTO event_assignments VALUES
(1, 101, 'Sato', 'sato@example.com'),
(1, 101, 'Ito',  'ito@example.com'),
(2, 102, 'Chen', 'chen@example.com'),
(2, 103, 'Chen', 'chen@example.com');

INSERT INTO artist_skills VALUES
(10, 'Arrangement', 'Guitar'),
(10, 'Arrangement', 'Piano'),
(10, 'Live',        'Guitar'),
(11, 'Mixing',      'Synth'),
(11, 'Mixing',      'Drums');

INSERT INTO supplier_parts_projects VALUES
('Acme',  'Sensor',  'Alpha', 'Initial build'),
('Acme',  'Sensor',  'Beta',  'Reuse'),
('Brill', 'Frame',   'Alpha', 'Custom order'),
('Brill', 'Frame',   'Gamma', 'Prototype'),
('Cyan',  'Sensor',  'Alpha', 'Backup');
```

### データ確認

```sql
SELECT * FROM event_assignments ORDER BY event_id, organizer;
SELECT * FROM artist_skills ORDER BY artist_id, skill;
SELECT * FROM supplier_parts_projects ORDER BY supplier, part, project;
```

ここでの違和感（重複や更新時の矛盾）を記録しておくと、後段の分解結果を評価しやすくなります。

---

## 1. ボイスコッド正規形 (BCNF) を体験する

### 1-1. 何が問題か

`event_assignments` では `hall_id -> organizer, contact` の部分関数従属性が存在します。すなわち、イベント会場 (hall_id) が決まると担当者が一意に決まり、イベントごとに行を保持するテーブル設計と矛盾しています。このままでは以下の問題が起こります。

- 会場の担当者が変わると、同じ会場を使うイベントすべてを更新する必要がある
- 会場担当者を削除したいだけでも、関連イベントが消えてしまう削除異常が発生

### 1-2. FROM 3NF TO BCNF

BCNF では「全ての決定項が候補キー」であることを要求します。`event_assignments` を以下の 2 テーブルに分割し、イベントと会場担当の従属性を切り離します。

```sql
CREATE TABLE events (
    event_id INT PRIMARY KEY,
    hall_id  INT
);

CREATE TABLE hall_contacts (
    hall_id  INT PRIMARY KEY,
    organizer TEXT,
    contact   TEXT
);

INSERT INTO events SELECT DISTINCT event_id, hall_id FROM event_assignments;
INSERT INTO hall_contacts SELECT DISTINCT hall_id, organizer, contact FROM event_assignments;
```

> 🔧 小技: `SELECT DISTINCT` で重複を一度に除去できますが、後で `PRIMARY KEY` 制約を付ける際に想定外の重複がないか `COUNT(*) - COUNT(DISTINCT ...)` で確認しておくと安全です。

### 1-3. 検証

```sql
SELECT e.event_id, e.hall_id, h.organizer, h.contact
FROM events e
JOIN hall_contacts h USING (hall_id)
ORDER BY e.event_id, h.organizer;
```

- 更新は `hall_contacts` の単一行で済む
- イベントを削除しても会場担当のマスタが残る

このように、**会場ごとの属性は hall_contacts に集約し、イベントテーブルからは冗長さを排除**できました。これが BCNF への分解効果です。

---

## 2. 第4正規形 (4NF) と多値従属性

### 2-1. 多値従属性とは？

`artist_skills` では、アーティストごとに複数のスキルと複数の担当楽器が独立に存在します。すなわち `artist_id ->> skill` と `artist_id ->> instrument` という多値従属性が同居しており、**1レコードで 2 種類の繰り返し属性を保持している状態**です。

### 2-2. 4NF への分解

多値従属性ごとにテーブルを分け、交差表を排除します。

```sql
CREATE TABLE artist_skill_map (
    artist_id INT,
    skill     TEXT,
    PRIMARY KEY (artist_id, skill)
);

CREATE TABLE artist_instrument_map (
    artist_id INT,
    instrument TEXT,
    PRIMARY KEY (artist_id, instrument)
);

INSERT INTO artist_skill_map
SELECT DISTINCT artist_id, skill FROM artist_skills;

INSERT INTO artist_instrument_map
SELECT DISTINCT artist_id, instrument FROM artist_skills;
```

### 2-3. 効果を確認

- スキル追加と楽器追加が独立に行える
- どちらか一方だけを削除しても、もう片方に影響しない

必要に応じてビューを作り、元の形の参照も可能です。

```sql
CREATE OR REPLACE VIEW artist_skills_view AS
SELECT s.artist_id, s.skill, i.instrument
FROM artist_skill_map s
JOIN artist_instrument_map i USING (artist_id);
```

---

## 3. 第5正規形 (5NF) と結合従属性

### 3-1. 5NF が必要な状況

`supplier_parts_projects` は「サプライヤーは特定の部品を供給でき、その部品はプロジェクトで使用される」という 3 方向のリレーションです。

- `supplier` と `part` の組 (供給実績)
- `part` と `project` の組 (採用実績)
- `supplier` と `project` の組 (取引実績)

これらの関係が独立に保持される場合、結合従属性が成立し 5NF への分解が可能です。

### 3-2. 5NF 分解の手順

1. `supplier_part`, `part_project`, `supplier_project` の 3 テーブルに分解
2. `supplier_parts_projects` はそれらの結合結果として再構成

```sql
CREATE TABLE supplier_part (
    supplier TEXT,
    part     TEXT,
    PRIMARY KEY (supplier, part)
);

CREATE TABLE part_project (
    part    TEXT,
    project TEXT,
    PRIMARY KEY (part, project)
);

CREATE TABLE supplier_project (
    supplier TEXT,
    project  TEXT,
    PRIMARY KEY (supplier, project)
);

INSERT INTO supplier_part
SELECT DISTINCT supplier, part FROM supplier_parts_projects;

INSERT INTO part_project
SELECT DISTINCT part, project FROM supplier_parts_projects;

INSERT INTO supplier_project
SELECT DISTINCT supplier, project FROM supplier_parts_projects;
```

> 🔧 小技: 分解後に `EXCEPT` で差集合を確認し、元テーブルと完全一致することを検証すると安心です。

```sql
SELECT supplier, part, project
FROM supplier_part sp
JOIN part_project pp USING (part)
JOIN supplier_project sj USING (supplier, project)
EXCEPT
SELECT supplier, part, project FROM supplier_parts_projects;
```

結果が 0 行であれば、分解前後で情報欠落がないことが確認できます。

---

## 4. 【実践演習】高位正規形チャレンジ

### 演習1: 会場割り当ての更新不要を証明 (難易度: ★★☆)

- `hall_contacts` の担当者を更新したとき、イベントテーブルに反映されることを確認せよ。
- `UPDATE hall_contacts SET organizer = 'New Lead' WHERE hall_id = 101;` を実行し、`events` との JOIN 結果が一貫しているかを確かめること。

> 💡ヒント: `BEGIN` ... `ROLLBACK` で実験を包むと、ハンズオン後に元のデータへ戻しやすくなります。

### 演習2: 多値従属性分解の副作用 (難易度: ★★☆)

- `artist_skill_map` のみを更新した場合に `artist_skills_view` がどう変わるか検証せよ。
- 「スキル1件 + 楽器2件」のケースで行数がどう変化するかを説明すること。

> 💡ヒント: ビューを `SELECT COUNT(*)` で素早く確認するクセを付けると、想定外の組み合わせ発生に気づけます。

### 演習3: 5NF 分解の完全性チェック (難易度: ★★★)

- `supplier_part`, `part_project`, `supplier_project` を結合して、元データとの差分がないことを SQL で示せ。
- 逆に差分があるケースを作り、どのテーブルが不足していたかを突き止めること。

> 💡ヒント: `INSERT` で意図的に 1 テーブルだけ更新し、`EXCEPT` の結果を観察すると結合従属性の感覚がつかめます。

---

## 5. 解答例

<details>
<summary><strong>演習1の解答例</strong></summary>

```sql
BEGIN;
UPDATE hall_contacts SET organizer = 'New Lead' WHERE hall_id = 101;
SELECT e.event_id, e.hall_id, h.organizer
FROM events e JOIN hall_contacts h USING (hall_id)
WHERE e.hall_id = 101;
ROLLBACK;
```

BCNF に分解したことで、`hall_contacts` だけ更新すれば JOIN 先のイベント情報も自動的に最新化される。もし単一テーブルのままだと、`event_assignments` の該当行すべてを更新する必要があった。

</details>

<details>
<summary><strong>演習2の解答例</strong></summary>

```sql
INSERT INTO artist_skill_map VALUES (11, 'Live');
SELECT * FROM artist_skills_view WHERE artist_id = 11;
DELETE FROM artist_skill_map WHERE artist_id = 11 AND skill = 'Live';
```

スキル1件を追加すると、`artist_instrument_map` の組み合わせ分だけ行が増える。多値従属性を切り離したことで、**「楽器×スキル」の直積がビューで自動生成される**ため、どの組み合わせが実在するかを制御したい場合は、ビューではなく別テーブルを用意する判断も必要になる。

</details>

<details>
<summary><strong>演習3の解答例</strong></summary>

```sql
WITH reconstructed AS (
    SELECT supplier, part, project
    FROM supplier_part sp
    JOIN part_project pp USING (part)
    JOIN supplier_project sj USING (supplier, project)
)
SELECT * FROM reconstructed
EXCEPT
SELECT supplier, part, project FROM supplier_parts_projects;
```

結果が 0 行なら完全復元に成功している。もし差分が出た場合は、欠けている組み合わせをもとに、どの分解テーブルが不足しているかを逆算できる。

</details>

---

## おわりに

BCNF、4NF、5NF を段階的に体験すると、**「どの異常を防ぎたいか」** が明確になり、正規化の粒度を感覚的に選べるようになります。実務では性能や分析要件とのトレードオフもありますが、分解のゴールを知っておくことで、わざと冗長化するケースでも根拠を持って判断できます。

本コンテンツの作成時間：約12時間
