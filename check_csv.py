import csv
rows = list(csv.DictReader(open('pc_amazon_feed_v4_20260420_121918.csv', encoding='utf-8', errors='replace')))
has_desc = sum(1 for r in rows if r.get('description','').strip())
print('Total rows:', len(rows))
print('Rows with description:', has_desc)
print('Columns:', list(rows[0].keys())[:10] if rows else 'none')
