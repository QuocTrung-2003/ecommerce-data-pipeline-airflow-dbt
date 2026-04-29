from fastapi import APIRouter, Request, HTTPException
from core.auth import require_auth
from core.rate_limit import check_rate_limit
from core.chaos import maybe_chaos
from services.data_service import get_data
from utils.pagination import paginate

router = APIRouter()

@router.get("/{resource}")
def list_resources(resource: str, request: Request):

    # validate
    if resource not in ["customers","orders","order_items","products","visits"]:
        raise HTTPException(status_code=404, detail="Not found")

    # middleware-like logic
    if not check_rate_limit():
        raise HTTPException(status_code=429, detail="rate_limited")

    require_auth(request)
    maybe_chaos()

    qs = dict(request.query_params)

    items = get_data(resource, qs)

    page = int(qs.get("page", 1))
    page_size = min(int(qs.get("page_size", 500)), 1000)

    return paginate(items, page, page_size)