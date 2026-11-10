# مصادر التحقق

- مكتبة الشموع التاريخية Python `tvdatafeed`: https://github.com/rongardF/tvdatafeed
  - تم اختبار `TvDatafeed().get_hist(symbol='XAUUSD', exchange='FX_IDC', interval=Interval.in_30_minute, n_bars=300)` في 10 نوفمبر 2026.
  - الاختبار أعاد 300 شمعة M30، وكان فهرس وقت FX_IDC بلا timezone وبقيم UTC (مثل آخر شمعة عند 20:00 UTC مع وقت الفحص 20:05 UTC). الكود يفسر الفهرس الساذج UTC ثم يستبعد الشمعة غير المكتملة عند بناء الإشارات.
- مكتبة TradingView المجتمعية JavaScript: https://github.com/Mathieu2301/TradingView-API
- توثيق Data API لدوال `getQuote`, `getQuotes`, `watchQuotes`: https://github.com/Mathieu2301/TradingView-API/blob/main/docs/data-api.md
  - تم اختبار `@mathieuc/tradingview` إصدار 4.1.1. دالة `watchQuotes({symbols:['FX_IDC:XAUUSD']}, handlers)` أرسلت تحديثات متتابعة تتضمن `lp`, `bid`, `ask` و`lp_time`.
- المكتبتان مجتمعيتان وليستا واجهة رسمية مضمونة من TradingView؛ يجب عدم افتراض أن السعر مطابق لكل وسيط أو أن التدفق غير متأخر. عند انقطاع التحديثات، يتوقف الكود عن تفعيل الإشارات أو اختبار الوقف إلى أن يصل اقتباس جديد.
