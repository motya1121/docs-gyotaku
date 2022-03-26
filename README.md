# docs-gyotaku


## Dynamo DB

### キーなど

- PartitionKey : HASH, プライマリキー
- SortKey: Sort, ソートキー
- LSI
  - SiteData
    - PartitionKey
    - timestamp

### 監視対象サイト

PartitionKey: site-@@@@@@@@@@
SortKey: site-@@@@@@@@@@
timestamp: 最終更新日時のタイムスタンプ(数値)
url: サイトのURL
type: Rss or msdocs
is_archive: 魚拓を取るかどうか
Property: プロパティ
tags: サイトのタグ

### サイトデータ

PartitionKey: site-@@@@@@@@@@
SortKey: ハッシュ値
timestamp: 最終更新日時のタイムスタンプ(数値)
url: サイトのURL

### 受信者

PartitionKey: user-@@@@@@@@@@
SortKey: メアド
tags: サイトのタグ

## タイムスタンプの計算

https://url-c.com/tc/

