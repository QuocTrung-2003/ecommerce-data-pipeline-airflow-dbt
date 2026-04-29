# ecommerce-data-pipeline-airflow-dbt/mock_api/api/routes.py
from fastapi import APIRouter, Request, HTTPException
from core.auth import require_auth
from core.rate_limit import check_rate_limit
from core.chaos import maybe_chaos
from services.data_service import get_data
from utils.pagination import paginate

router = APIRouter()

ALLOWED_FACTS = {"orders", "visits"}
ALLOWED_DIMS = {"customers", "products", "order_items"}

def safe_int(v, default):
    try:
        return int(v)
    except:
        return default


# ---------------------------
# RAW DATA ACCESS (restricted)
# ---------------------------
@router.get("/data/{resource}")
def raw_resource(resource: str, request: Request):

    if resource not in ALLOWED_FACTS | ALLOWED_DIMS:
        raise HTTPException(status_code=404, detail="Not found")

    if not check_rate_limit():
        raise HTTPException(status_code=429, detail="rate_limited")

    require_auth(request)
    maybe_chaos()

    qs = dict(request.query_params)
    items = get_data(resource, qs)

    return {
        "count": len(items),
        "data": items[:1000]   # safety cap
    }


# ---------------------------
# OLAP KPI ENDPOINT
# ---------------------------
@router.get("/analytics/kpi")
def kpi(request: Request):

    qs = dict(request.query_params)
    orders = get_data("orders", qs)

    revenue = sum(o["total_amount"] for o in orders)
    count_orders = len(orders)
    aov = revenue / count_orders if count_orders else 0

    return {
        "revenue": revenue,
        "orders": count_orders,
        "aov": round(aov, 2)
    }


# ---------------------------
# TIME SERIES (BI CORE)
# ---------------------------
@router.get("/analytics/revenue/daily")
def revenue_daily(request: Request):

    qs = dict(request.query_params)
    orders = get_data("orders", qs)

    result = {}

    for o in orders:
        day = o["created_at"][:10]
        result[day] = result.get(day, 0) + o["total_amount"]

    return result


# ---------------------------
# FUNNEL SIMULATION (OLAP STYLE)
# ---------------------------
@router.get("/analytics/funnel")
def funnel(request: Request):

    qs = dict(request.query_params)

    visits = get_data("visits", qs)
    orders = get_data("orders", qs)

    return {
        "visits": len(visits),
        "orders": len(orders),
        "conversion_rate": len(orders) / len(visits) if visits else 0
    }