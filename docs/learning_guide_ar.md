# دليل التعلّم (بالعربية)

> الشيفرة والتوثيق التقني بالإنجليزية كما طُلب. هذا الملف شرح تعليمي فقط.

## كيف يفكّر مهندس البيانات في هذا المشروع؟

1. **ابدأ من المشكلة لا من الأدوات.** المشكلة: ميزات (features) تُحسب بطريقتين مختلفتين في التدريب والتشغيل، وتسرّب بيانات المستقبل إلى الماضي، وخطوط معالجة لا يمكن إعادة تشغيلها بأمان. كل قرار في الهيكل يخدم واحدة من هذه الثلاث.
2. **حدّد الحدود بين الوحدات.** كل مجلد في `src/featureforge` له مسؤولية واحدة (إدخال، تنظيف، ميزات، جودة، تخزين، تنسيق). القاعدة: منطق الأعمال لا يعرف أين يُنفَّذ (Prefect أو Python عادي أو AWS).
3. **اختر المكتبة بحسب المشكلة.** DuckDB لأن حسابات النوافذ الزمنية أوضح بـ SQL وتقرأ Parquet بكسل وتتسرّب للقرص عند امتلاء الذاكرة؛ Polars للتنظيف؛ Pandera للمخطط؛ Prefect لإعادة المحاولة والحالة؛ Terraform لأن البنية يجب أن تكون قابلة للمراجعة.
4. **اجعل الخطأ مكلفاً مبكراً.** فشل حرج في الجودة يوقف النشر قبل أن تصل بيانات سيئة للمستهلكين.

## المفاهيم الأساسية التي يجب أن تفهمها

| المفهوم | الفكرة في جملة | أين تراه |
|---|---|---|
| زمنان لكل سجل | زمن الحدث (`event_ts`) مختلف عن زمن توفّر السجل (`available_at`) | `docs/point_in_time.md` |
| As-of join | لكل ملاحظة عند الزمن T نأخذ آخر لقطة `snapshot_ts <= T` فقط | `FeatureStore.get_training_features` |
| Idempotency | تشغيل نفس الدفعة مرتين = نفس النتيجة بلا تكرار | `batch_id` في `steps.publish_feature_snapshots` |
| Immutable raw | البيانات الخام لا تُعدَّل، التصحيحات نسخ جديدة | `ingestion/ingest.py` |
| Partitioned Parquet | مجلد لكل تاريخ لقطة → استعلام "كما في تاريخ" سريع | `feature_store/` |
| Watermark | الإدخال التزايدي يأخذ فقط ما بعد آخر نقطة وصلنا إليها | `previous_watermark` |
| Quarantine | الصفوف المرفوضة تُحفظ مع السبب ولا تُحذف بصمت | `rejected/` |

## كيف تختبر كل جزء بنفسك

```bash
uv run pytest tests/unit/test_point_in_time.py -q      # افهم التسرّب أولاً
uv run featureforge backfill --start 2025-04-01 --end 2025-04-29 --step-days 7 --no-prefect
uv run featureforge run --date 2025-04-01 --no-prefect  # لاحظ: skipped_duplicate
uv run featureforge query "SELECT * FROM customer_features LIMIT 5"
```

تمرين: عدّل كمية سجل في المصدر ثم جرّب `restate` و`stale` ولاحظ أي لقطات تتأثر ولماذا.

## ما الذي تدرسه (دون حفظ كل شيء)

* DuckDB: window functions, `FILTER`, `QUALIFY`, `read_parquet` مع hive partitioning.
* Polars: `scan_parquet` (lazy) و`join_asof`.
* Prefect 3: `@flow`/`@task`، `retries`، `retry_condition_fn`.
* Terraform: الملف `infrastructure/terraform/main.tf` من الأعلى للأسفل: S3 → IAM → ECS → Scheduler.
* مفهوم Point-in-time من وثيقة الـ feature store في أي منصة (Feast مثلاً) للمقارنة.

## تحذيرات صادقة

* البيانات الافتراضية **اصطناعية**؛ أرقام النموذج تثبت سلامة الأنابيب لا قدرة تنبؤية حقيقية.
* لم يُنشر شيء على AWS؛ راجع `docs/cloud_deployment.md` قبل أي تنفيذ وتأكد من التكاليف.
