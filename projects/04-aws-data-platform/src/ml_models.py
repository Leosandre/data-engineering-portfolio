"""
Gera features pra ML a partir dos dados reconciliados.
Treina 2 modelos:
  1. Forecast de vendas (Prophet) — serie temporal semanal
  2. Churn prediction (XGBoost) — classificacao por cliente

Roda local como prova de conceito. Em producao, roda no SageMaker.
"""

import logging
from pathlib import Path
from datetime import datetime

import polars as pl
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
SYN = BASE / "data" / "synthetic"
REC = BASE / "data" / "reconciled"
ML = BASE / "data" / "ml"

CHURN_DAYS = 90  # sem compra nos ultimos 90 dias = churned
REFERENCE_DATE = datetime(2024, 12, 31)


def build_rfm_features():
    """Calcula Recency, Frequency, Monetary por cliente."""
    log.info("Construindo features RFM...")

    orders = pl.read_parquet(SYN / "erp_orders.parquet")
    items = pl.read_parquet(SYN / "erp_order_items.parquet")
    golden = pl.read_parquet(REC / "golden_customers.parquet")

    # so pedidos completados
    orders = orders.filter(pl.col("status") == "completed")

    # agregar por cliente
    rfm = orders.group_by("customer_id").agg([
        # recency: dias desde ultima compra
        (pl.lit(REFERENCE_DATE) - pl.col("order_date").str.to_datetime("%Y-%m-%dT%H:%M:%S").max()).dt.total_days().alias("recency_days"),
        # frequency: numero de pedidos
        pl.len().alias("frequency"),
        # monetary: valor total
        pl.col("total_amount").sum().alias("monetary"),
        # ticket medio
        pl.col("total_amount").mean().alias("avg_ticket"),
        # primeiro e ultimo pedido
        pl.col("order_date").str.to_datetime("%Y-%m-%dT%H:%M:%S").min().alias("first_order"),
        pl.col("order_date").str.to_datetime("%Y-%m-%dT%H:%M:%S").max().alias("last_order"),
    ])

    # tempo como cliente (dias entre primeiro e ultimo pedido)
    rfm = rfm.with_columns(
        (pl.col("last_order") - pl.col("first_order")).dt.total_days().alias("tenure_days")
    )

    # frequencia mensal (pedidos / meses como cliente)
    rfm = rfm.with_columns(
        (pl.col("frequency") / (pl.col("tenure_days").cast(pl.Float64) / 30.0).clip(1, None)).alias("monthly_frequency")
    )

    # tendencia: valor dos ultimos 3 meses vs 3 meses anteriores
    cutoff = REFERENCE_DATE - pd.Timedelta(days=90)
    recent = orders.filter(pl.col("order_date").str.to_datetime("%Y-%m-%dT%H:%M:%S") >= cutoff) \
        .group_by("customer_id").agg(pl.col("total_amount").sum().alias("recent_monetary"))
    older = orders.filter(pl.col("order_date").str.to_datetime("%Y-%m-%dT%H:%M:%S") < cutoff) \
        .group_by("customer_id").agg(pl.col("total_amount").mean().alias("older_avg_monetary"))

    rfm = rfm.join(recent, on="customer_id", how="left") \
             .join(older, on="customer_id", how="left")

    rfm = rfm.with_columns(
        (pl.col("recent_monetary").fill_null(0) / pl.col("older_avg_monetary").fill_null(1).clip(1, None)).alias("spend_trend")
    )

    # churn label: usa ground truth (gerado pelo simulador)
    truth = pl.read_parquet(SYN / "_ground_truth.parquet").select("customer_id", pl.col("churned").alias("churned_truth"))
    rfm = rfm.join(truth, on="customer_id", how="left")
    rfm = rfm.with_columns(pl.col("churned_truth").fill_null(False).alias("churned"))
    rfm = rfm.drop("churned_truth")

    # join com golden pra pegar segmento e flag has_crm
    rfm = rfm.join(
        golden.select("erp_customer_id", "segment", "has_crm"),
        left_on="customer_id", right_on="erp_customer_id", how="left"
    )

    log.info("  RFM features: %d clientes", len(rfm))
    log.info("  Churn rate: %.1f%%", 100 * rfm["churned"].sum() / len(rfm))

    return rfm


def train_churn_model(rfm):
    """Treina XGBoost pra churn prediction."""
    log.info("Treinando modelo de churn (XGBoost)...")

    try:
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import classification_report, roc_auc_score
        import xgboost as xgb
    except ImportError:
        log.warning("sklearn/xgboost nao instalado. Instalando...")
        import subprocess
        subprocess.check_call(["pip", "install", "scikit-learn", "xgboost", "-q"])
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import classification_report, roc_auc_score
        import xgboost as xgb

    feature_cols = ["recency_days", "frequency", "monetary", "avg_ticket",
                    "tenure_days", "monthly_frequency", "spend_trend"]

    df = rfm.select(feature_cols + ["churned", "customer_id"]).to_pandas()
    df = df.dropna(subset=feature_cols)

    X = df[feature_cols]
    y = df["churned"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    model = xgb.XGBClassifier(
        n_estimators=100, max_depth=5, learning_rate=0.1,
        scale_pos_weight=len(y_train[y_train == 0]) / max(len(y_train[y_train == 1]), 1),
        random_state=42, eval_metric="logloss",
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    auc = roc_auc_score(y_test, y_prob)
    log.info("  AUC-ROC: %.3f", auc)
    log.info("\n%s", classification_report(y_test, y_pred, target_names=["ativo", "churned"]))

    # feature importance
    importance = dict(zip(feature_cols, model.feature_importances_))
    log.info("  Feature importance:")
    for feat, imp in sorted(importance.items(), key=lambda x: -x[1]):
        log.info("    %s: %.3f", feat, imp)

    # scores pra todos os clientes
    all_probs = model.predict_proba(df[feature_cols])[:, 1]
    scores = pl.DataFrame({
        "customer_id": df["customer_id"].values,
        "churn_probability": all_probs,
        "churn_risk": ["alto" if p > 0.7 else "medio" if p > 0.4 else "baixo" for p in all_probs],
    })

    scores.write_parquet(ML / "churn_scores.parquet")
    log.info("  Churn scores salvos: %d clientes", len(scores))

    # salvar metricas
    metrics = pl.DataFrame([{
        "model": "churn_xgboost",
        "auc_roc": round(auc, 3),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "churn_rate_train": round(y_train.mean(), 3),
        "churn_rate_test": round(y_test.mean(), 3),
        "top_feature": max(importance, key=importance.get),
    }])
    metrics.write_parquet(ML / "churn_metrics.parquet")

    return model, importance


def train_forecast_model():
    """Treina Prophet pra forecast de vendas."""
    log.info("Treinando modelo de forecast (Prophet)...")

    try:
        from prophet import Prophet
    except ImportError:
        log.warning("prophet nao instalado. Instalando...")
        import subprocess
        subprocess.check_call(["pip", "install", "prophet", "-q"])
        from prophet import Prophet

    orders = pl.read_parquet(SYN / "erp_orders.parquet")
    orders = orders.filter(pl.col("status") == "completed")

    # agregar por semana
    weekly = orders.with_columns(
        pl.col("order_date").str.to_datetime("%Y-%m-%dT%H:%M:%S").dt.truncate("1w").alias("week")
    ).group_by("week").agg([
        pl.col("total_amount").sum().alias("revenue"),
        pl.len().alias("n_orders"),
    ]).sort("week")

    # Prophet espera colunas ds e y
    pdf = weekly.select(
        pl.col("week").alias("ds"),
        pl.col("revenue").alias("y"),
    ).to_pandas()

    model = Prophet(weekly_seasonality=True, yearly_seasonality=True, daily_seasonality=False)
    model.fit(pdf)

    # forecast 6 meses (26 semanas)
    future = model.make_future_dataframe(periods=26, freq="W")
    forecast = model.predict(future)

    result = pl.DataFrame({
        "week": forecast["ds"].values,
        "revenue_forecast": forecast["yhat"].values,
        "revenue_lower": forecast["yhat_lower"].values,
        "revenue_upper": forecast["yhat_upper"].values,
    })

    # juntar com dados reais
    actual = weekly.select(
        pl.col("week").cast(pl.Datetime("ns")),
        pl.col("revenue").alias("revenue_actual"),
    )
    result = result.with_columns(pl.col("week").cast(pl.Datetime("ns")))
    result = result.join(actual, on="week", how="left")

    result.write_parquet(ML / "forecast_sales.parquet")
    log.info("  Forecast salvo: %d semanas (incluindo %d futuras = 6 meses)", len(result), 26)

    # metricas no periodo historico
    hist = result.filter(pl.col("revenue_actual").is_not_null())
    mape = (((hist["revenue_actual"] - hist["revenue_forecast"]).abs() / hist["revenue_actual"]).mean()) * 100
    log.info("  MAPE historico: %.1f%%", mape)

    metrics = pl.DataFrame([{
        "model": "forecast_prophet",
        "mape": round(mape, 1),
        "n_weeks_train": len(pdf),
        "n_weeks_forecast": 13,
    }])
    metrics.write_parquet(ML / "forecast_metrics.parquet")

    return model


if __name__ == "__main__":
    ML.mkdir(parents=True, exist_ok=True)

    rfm = build_rfm_features()
    rfm.write_parquet(ML / "rfm_features.parquet")

    train_churn_model(rfm)
    train_forecast_model()

    log.info("ML concluido.")
