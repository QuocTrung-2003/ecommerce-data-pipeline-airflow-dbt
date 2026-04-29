def paginate(items, page, page_size):
    total = len(items)
    total_pages = (total + page_size - 1) // page_size

    start = (page - 1) * page_size
    end = start + page_size

    return {
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "next_page": page + 1 if page < total_pages else None,
        "count": len(items[start:end]),
        "data": items[start:end],
    }