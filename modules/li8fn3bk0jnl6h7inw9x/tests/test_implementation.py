from li8fn3bk0jnl6h7inw9x import query
from exorde_data import Item
import pytest

@pytest.mark.asyncio
async def test_query():
    query_dic = {"keyword": "bitcoin"}
    results = []
    async for result in query(query_dic):
        assert isinstance(result, Item)
        results.append(result)
