# آرشیو روزانهٔ بورس تهران

هر روز معاملاتی چند فایل فشرده دارد (CSV با UTF-8؛ روزهای کامل `.csv.xz`، روز جاری `.csv.gz`):
- **معاملات**: هر 60 ثانیه در ساعات بازار، فقط نمادهایی که نسبت به ثبت قبلی تغییر کرده‌اند.
  آخرین ردیف هر نماد تا یک زمان مشخص = وضعیت آن نماد در آن لحظه. ستون «زمان دریافت» به وقت تهران است.
- **شاخص‌ها**: مقدار همهٔ شاخص‌ها در همان زمان‌ها.
- **اطلاعات ثابت**: گروه، تعداد سهام، شناوری، میانگین حجم ماه، مشخصات اختیارها و ... در پایان روز.
- **هشدارها، کدال، خبرها، دلار و طلا** (`_alerts`، `_codal`، `_news`، `_macro`): هر مورد با `fetched_at_utc`.

خواندن با Python:
```python
import pandas as pd
df = pd.read_csv("https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-01.csv.xz")      # pandas نوع فشرده‌سازی را از پسوند تشخیص می‌دهد
```

## روزهای موجود در همین مخزن (2 روز؛ 120 روز آخر نگه داشته می‌شود)

- 2026-10-04: [معاملات](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-04.csv.gz) (0.03 MB) | [شاخص‌ها](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-04_indices.csv.gz) (0.01 MB) | [کدال](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-04_codal.csv.gz) (0.01 MB) | [خبرها](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-04_news.csv.gz) (0.12 MB) | [دلار و طلا](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-04_macro.csv.gz) (0.01 MB) — در حال تکمیل
- 2026-10-03: [معاملات](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03.csv.xz) (6.36 MB) | [شاخص‌ها](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03_indices.csv.xz) (0.03 MB) | [اطلاعات ثابت](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03_static.csv.xz) (0.09 MB) | [هشدارها](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03_alerts.csv.xz) (0.00 MB) | [کدال](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03_codal.csv.xz) (0.01 MB) | [خبرها](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03_news.csv.xz) (0.05 MB) | [دلار و طلا](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03_macro.csv.xz) (0.00 MB) | [حکم‌های ارسال‌شده](https://raw.githubusercontent.com/alitamizi/tse-data/main/archive/2026-10-03_verdicts.csv.xz) (0.00 MB) — کامل

## آرشیو دائمی (GitHub Releases)
همهٔ روزهای کامل، از جمله روزهای قدیمی‌تر از 120 روز، به‌صورت ماهانه در Releases نگه داشته می‌شوند:
https://github.com/alitamizi/tse-data/releases

آدرس دانلود مستقیم: `https://github.com/alitamizi/tse-data/releases/download/archive-YYYY-MM/YYYY-MM-DD.csv.xz` (و `_indices`، `_static`)

- 2026-10-03: [معاملات](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03.csv.xz) | [شاخص‌ها](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03_indices.csv.xz) | [اطلاعات ثابت](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03_static.csv.xz) | [هشدارها](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03_alerts.csv.xz) | [کدال](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03_codal.csv.xz) | [خبرها](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03_news.csv.xz) | [دلار و طلا](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03_macro.csv.xz) | [حکم‌های ارسال‌شده](https://github.com/alitamizi/tse-data/releases/download/archive-2026-10/2026-10-03_verdicts.csv.xz)

## نسخهٔ کامل روی کامپیوتر ربات
همهٔ روزها بدون حذف در پوشهٔ `output/archive` کنار برنامه هم ذخیره می‌شوند.
