# داده‌های لحظه‌ای بورس تهران (TSETMC)

- **زمان انتشار این صفحه (UTC): `2026-10-04T06:00:48Z`** — تهران: `2026-10-04T09:30:48+03:30`
- زمان دریافت داده‌های این مخزن از بورس (UTC): `2026-10-04T06:00:48Z` — تهران: `2026-10-04T09:30:48+03:30`
- تعداد نمادهای معامله‌شده: 1355

## راهنما برای Claude
1. **اول اتصال زنده** (تأخیر چندثانیه‌ای). آدرس‌ها به ترتیب اولویت:

   - اولویت 1: https://b96fa1a2a95801.lhr.life  — فعال (آخرین بررسی موفق: 2026-10-04T06:00:27Z)
   - اولویت 2: https://50c5e705bb2af013-80-210-48-94.serveousercontent.com  — متصل، ولی از داخل ایران قابل بررسی نیست؛ ممکن است از بیرون کار کند

   صفحهٔ اصلی هر آدرس راهنما و پیوند همهٔ بخش‌ها را دارد. اگر اولی جواب نداد یا خطا داد، بعدی را امتحان کن.
   📊 **برنامهٔ تحلیل بازه‌ای** (در مرورگر): `<آدرس زنده>/app` — API آن برای Claude: `<آدرس زنده>/api/range?day=...&from=10:00&to=10:10&symbols=خودرو`
   هر پاسخ زمان دریافت از بورس (`fetched_at_utc`) و زمان ارسال (`served_at_utc`) را دارد.
   اگر `served_at_utc` قدیمی بود، ابزار خواندن صفحه نسخهٔ کش‌شده را داده است؛ همان آدرس را با پسوند تازه
   (`?r=2`، `?r=3`، ...) دوباره بخوان.
2. **اگر هیچ اتصال زنده‌ای کار نکرد**، از فایل‌های همین مخزن بخوان (حدود هر ۳۰ ثانیه در ساعات بازار به‌روز می‌شوند):
   - خلاصهٔ بازار و برترین‌ها: https://github.com/alitamizi/tse-data/blob/main/data/summary.md
   - همهٔ نمادها با پیوند به هر نماد (هر ۲ دقیقه): https://github.com/alitamizi/tse-data/blob/main/data/all.md
   - یک نماد: https://github.com/alitamizi/tse-data/blob/main/data/symbols/<نماد>.json — زمان دریافتش همان `fetched_at_utc` در https://github.com/alitamizi/tse-data/blob/main/data/meta.json است
   - روند امروز یک نماد (هر ۲ دقیقه): https://github.com/alitamizi/tse-data/blob/main/data/history/<نماد>.json
   - شاخص‌ها: https://github.com/alitamizi/tse-data/blob/main/data/indices.md
   - هشدارهای امروز: https://github.com/alitamizi/tse-data/blob/main/data/alerts.csv (وضعیت موتور هشدار: data/alerts_status.json)
   - اطلاعیه‌های کدال: https://github.com/alitamizi/tse-data/blob/main/data/codal.json | خبرها: https://github.com/alitamizi/tse-data/blob/main/data/news.json | دلار، سکه، طلا: https://github.com/alitamizi/tse-data/blob/main/data/macro.json
3. **آرشیو روزهای قبل** (CSV فشرده با xz/gz، برای تحلیل با کد): https://github.com/alitamizi/tse-data/blob/main/archive/index.md
   - همهٔ ستون‌ها (CSV، تا ۵ دقیقه کش می‌شود): https://raw.githubusercontent.com/alitamizi/tse-data/main/data/latest.csv
4. همیشه تأخیر را گزارش کن: (زمان فعلی UTC) − `fetched_at_utc`. اگر «زمان انتشار این صفحه» خیلی قدیمی است،
   ربات خاموش است یا بازار بسته است (شنبه تا چهارشنبه ۹:۰۰ تا ۱۲:۳۰ تهران).
5. در نام نمادها «ی» و «ک» فارسی‌اند و نیم‌فاصله حذف شده است.

## ستون‌ها
- قیمت و ارزش به ریال.
- «ارزش خرید/فروش حقیقی/حقوقی» و «ورود پول حقیقی» تقریبی‌اند (حجم × قیمت پایانی).
- «قدرت خریدار حقیقی» = سرانهٔ خرید حقیقی ÷ سرانهٔ فروش حقیقی (دقیق).
- «تقاضاN/عرضهN» = ردیف N دفتر سفارش. «صف» = صف خرید/فروش.
- «زمان آخرین معامله» = زمان آخرین معاملهٔ همان نماد، به وقت تهران.

## زمان‌ها (استاندارد ISO 8601)
- `...Z` یعنی UTC و `...+03:30` یعنی وقت تهران (ایران تغییر ساعت تابستانی ندارد).
- `fetched_at_utc` = لحظه‌ای که ربات این داده را از سرور بورس گرفت.
- `served_at_utc` = لحظه‌ای که این پاسخ فرستاده شد (فقط در اتصال زنده).
- تأخیر داده = (زمان فعلی UTC) − `fetched_at_utc`.
- ساعت ربات با ساعت سرور GitHub هم‌تراز می‌شود تا ساعت غلط کامپیوتر اثری نداشته باشد (`clock_synced_with_server`).
- `زمان آخرین معامله` هر نماد نشان می‌دهد داده‌ی بورس تا چه لحظه‌ای را شامل می‌شود.
